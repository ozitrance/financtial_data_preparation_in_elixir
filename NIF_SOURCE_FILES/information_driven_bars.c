#include <erl_nif.h> // Needed for our Erlang NIF Functions
#include <math.h> // Needed for our Math functions (pow, fabs, fmin, fmax)

// Calculating Exponentially Weighted Moving Average
static double calculate_ewma(const double* values, const size_t size, const double alpha) {
    // If size of our data is 0 (no elements in data) there's nothing to calculate so we just return 0.0
    if (size == 0) {
        return 0.0;
    }

    double ewma_old = values[0]; // We start with the first value in the list
    double weight = 1.0; // The first weight is 1.0 - then we keep adding to the most recent value has the biggest weight
    double ewma_value = ewma_old / weight; // Calculating our first ema_value

    // Looping over the data starting with 1 since we already calculated the first value
    // Updating our weight and ema_value every cycle
    for (size_t i = 1; i < size; ++i) {
        double power = pow(1.0 - alpha, i);
        weight += power;
        ewma_old = ewma_old * (1.0 - alpha) + values[i];
        ewma_value = ewma_old / weight;
    }
    // Returning the value from the last cycle
    return ewma_value;
}

static double get_expected_imbalance(double* values, size_t size, int window) {
    // Updating our window in case it's bigger than our values array size
    size_t actual_window = (size < (size_t)window) ? size : (size_t)window;
    // Finding the start point of the slice we will use according to the window 
    size_t start = (size >= window) ? size - actual_window : 0;
    // Getting a new size for the slice
    size = size - start;

    // Creating a pointer *slice and pointing it to the start point in the values array
    double *slice = &values[start];
    
    // Calculating the EWMA value and returning it
    double alpha = 2.0 / (actual_window + 1.0); // Calculating Alpha for EWMA
    double expected_imbalance = calculate_ewma(slice, size, alpha); 
    return expected_imbalance;
}

ERL_NIF_TERM compute_imbalance_bars(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {

    // Setting variables for our arguments Starting with our data iovec
    ERL_NIF_TERM iovec_term = argv[0];
    ErlNifIOVec* iovec = NULL;
    ERL_NIF_TERM tail;

    // The next 3 arguments, simple types
    int num_prev_bars;
    double expected_imbalance_window;
    double expected_num_ticks;
    
    // The 4th argument is a tuple with 2 items (arity/2)
    const ERL_NIF_TERM* constraints_tuple;
    int arity;

    // And the last argument is a Bool - 0/1 - false/true
    int ema_bars;

    // We get the arguments out so we can actually use them
    if (!enif_inspect_iovec(env, 0, iovec_term, &tail, &iovec) ||
        !enif_get_int(env, argv[1], &num_prev_bars) ||
        !enif_get_double(env, argv[2], &expected_imbalance_window) ||
        !enif_get_double(env, argv[3], &expected_num_ticks) ||
        !enif_get_tuple(env, argv[4], &arity, &constraints_tuple) || arity != 2 ||
        !enif_get_int(env, argv[5], &ema_bars)) {
        return enif_make_badarg(env);
    }

    // If ema_bars is true we extract the constraints out of the tuple, otherwise they're not needed.
    double expected_num_ticks_constraints_min;
    double expected_num_ticks_constraints_max;
    if (ema_bars) {
        if (!enif_get_double(env, constraints_tuple[0], &expected_num_ticks_constraints_min) ||
            !enif_get_double(env, constraints_tuple[1], &expected_num_ticks_constraints_max)) {
            return enif_make_badarg(env);
        }
    }

    size_t total_input_bytes = iovec->size; // Getting the full size of our iovec term
    size_t total_num_values = total_input_bytes / sizeof(double); // And calculating the number of values according to their size (sizeof double in this case)

    // Allocate the output binary so we can write to it in 1 pass while we process the input data
    ERL_NIF_TERM result;
    unsigned char* out_data_raw = enif_make_new_binary(env, total_num_values * sizeof(int64_t), &result);
    int64_t* out_data = (int64_t*)out_data_raw; // Casting our data to int64 - our bar numbers / simple integers

    // Setting a few initial values
    const double num_prev_bars_alpha = 2.0 / (num_prev_bars + 1.0); // This will never change
    int cum_ticks = 0; // Keeping track of how many values per bar
    int bar_number = 0; // And the current bar number 
    size_t global_index = 0; // Keep track of index since C doesn't have built-in size/length for arrays

    double cum_theta = 0.0; // Our cumulative imbalance value
    double expected_imbalance = 0.0; // Our expected imbalance to create new bars
    u_int8_t expected_imbalance_initialized = 0; // True / False value for the 'warmup' before the 1st bar

    // Allocate memory for arrays using enif_alloc instead of malloc hoping to squeeze a little optimization?
    // recent_imbalances will be updated on each loop so same amount of original values
    double* recent_imbalances = (double*)enif_alloc(total_num_values * sizeof(double));
    // If ema_bars is true, we allocate enough space just in case, altough we can probably use less 
    // if ema_bars is false we don't need num_ticks_bar so set it to 0 allocation, 
    double* num_ticks_per_bar = (double*)enif_alloc(ema_bars ? total_num_values * sizeof(double) : 0);
    size_t num_ticks_per_bar_index = 0; // Keep track of index since C doesn't have built-in size/length for arrays

    // Quick check to make sure memory allocation worked ok
    if (num_ticks_per_bar == NULL || recent_imbalances == NULL) {
        enif_free(num_ticks_per_bar);
        enif_free(recent_imbalances);
        return enif_make_badarg(env);
    }

    // Starting our loop with number of SysIOVec items in our iovec term - that's where our data is
    // Usually there's only 1 but it's not guaranteed so our data can be split over multiple SysIOVec 'bins'
    for (unsigned int i = 0; i < iovec->iovcnt; ++i)
    {   
        // Getting the current SysIOVec item and calculating the number of items according to sizeof type double
        SysIOVec* sysiov = &iovec->iov[i];
        size_t len = sysiov->iov_len;
        size_t num_values_in_bin = len / sizeof(double);
        const double* data = (const double*)sysiov->iov_base; // Pointing the SysIOVec data to 'data' pointer and casting to double

        // Looping through each bin and updating global_index on each loop
        for (size_t j = 0; j < num_values_in_bin; ++j, ++global_index)
        {
            double imbalance = data[j]; // Current imbalance

            // Few updates for each loop
            cum_theta += imbalance; // Our cumulative imbalance value
            out_data[global_index] = bar_number; // Outputing the bar number for this cycle
            recent_imbalances[global_index] = imbalance; // And adding the imbalance to this array so we can easily use it for calculations later
            ++cum_ticks; // Number of ticks (we could just use global_index - 1 but it was looking too confusing)

            // We use a 'warmup' period (expected_imbalance_initialized) to not continue processing before we have enough values (expected_num_ticks)
            if (!expected_imbalance_initialized && cum_ticks >= expected_num_ticks) {
                // Calculating expected_imbalance for the first time. global_index + 1 is the size of our recent_imbalances array
                expected_imbalance = get_expected_imbalance(recent_imbalances, global_index + 1, (int)expected_imbalance_window);
                expected_imbalance_initialized = 1;
            }
            // If still in 'warmup' continue to next cycle
            if (!expected_imbalance_initialized) continue;

            // Creating NEW BAR if our threshold reached
            if (fabs(cum_theta) > expected_num_ticks * fabs(expected_imbalance)) {
                // If we're creating ema_bars we update expected_num_ticks as well
                if (ema_bars) {
                    num_ticks_per_bar[num_ticks_per_bar_index] = (double)cum_ticks;
                    ++num_ticks_per_bar_index;
                    // Just like in calculate_ewma, we update the start and size of the slice we use according to window (num_prev_bars) and current size of our num_ticks_per_bar array 
                    size_t start = (num_ticks_per_bar_index > num_prev_bars) ? num_ticks_per_bar_index - num_prev_bars : 0;
                    size_t size = num_ticks_per_bar_index - start;
                    double *slice = &num_ticks_per_bar[start];
                    double ewma_value = calculate_ewma(slice, size, num_prev_bars_alpha);
                    // Clamping the expected_num_ticks value according to our constraints to prevent values getting too big or too small
                    expected_num_ticks = fmin(expected_num_ticks_constraints_max, fmax(ewma_value, expected_num_ticks_constraints_min));
                }

                // Update expected imbalance, global_index + 1 is the size of our recent_imbalances array (we update it when the loop cycle starts while the global_index updates on cycle end)
                expected_imbalance = get_expected_imbalance(recent_imbalances, global_index + 1, (int)expected_imbalance_window);

                // Reset cumulative variables
                cum_theta = 0.0;
                cum_ticks = 0;
                // And increment Bar Number to reflect the new bar
                ++bar_number;
            }
        }
    }

    // Free allocated memory
    enif_free(num_ticks_per_bar);
    enif_free(recent_imbalances);

    // And return our result
    return result;
}


ERL_NIF_TERM compute_run_bars(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {

    ERL_NIF_TERM iovec_term = argv[0];
    ErlNifIOVec* iovec = NULL;
    ERL_NIF_TERM tail;

    int num_prev_bars;
    double expected_imbalance_window;
    double expected_num_ticks;
    
    const ERL_NIF_TERM* constraints_tuple;
    int arity;

    int ema_bars;

    if (!enif_inspect_iovec(env, 0, iovec_term, &tail, &iovec) ||
        !enif_get_int(env, argv[1], &num_prev_bars) ||
        !enif_get_double(env, argv[2], &expected_imbalance_window) ||
        !enif_get_double(env, argv[3], &expected_num_ticks) ||
        !enif_get_tuple(env, argv[4], &arity, &constraints_tuple) || arity != 2 ||
        !enif_get_int(env, argv[5], &ema_bars)) {
        return enif_make_badarg(env);
    }

    double expected_num_ticks_constraints_min;
    double expected_num_ticks_constraints_max;
    if (ema_bars) {
        if (!enif_get_double(env, constraints_tuple[0], &expected_num_ticks_constraints_min) ||
            !enif_get_double(env, constraints_tuple[1], &expected_num_ticks_constraints_max)) {
            return enif_make_badarg(env);
        }
    }

    size_t total_input_bytes = iovec->size; 
    size_t total_num_values = total_input_bytes / sizeof(double); 

    ERL_NIF_TERM result;
    unsigned char* out_data_raw = enif_make_new_binary(env, total_num_values * sizeof(int64_t), &result);
    int64_t* out_data = (int64_t*)out_data_raw;

    const double num_prev_bars_alpha = 2.0 / (num_prev_bars + 1.0); 
    int cum_ticks = 0;
    int bar_number = 0;
    size_t global_index = 0; 


    // Up to here everything is the same as in copmute_imbalance_bars


    // This time we calculate our cumulative imbalances separately for buy or sell
    double cum_theta_buy = 0.0;
    double cum_theta_sell = 0.0;
    // Also keep track of how many buy_ticks. sell ticks is just cum_ticks - buy_ticks_num
    int buy_ticks_num = 0;

    // This part is similar to the one in copmute_imbalance_bars, we just use seperate arrays for buy and sell, and create indexes to keep track of size since we cannot use global_index anymore
    double* imbalance_array_buy = (double*)enif_alloc(total_num_values * sizeof(double));
    size_t imbalance_array_buy_index = 0;
    double* imbalance_array_sell = (double*)enif_alloc(total_num_values * sizeof(double));
    size_t imbalance_array_sell_index = 0;
    double* num_ticks_per_bar = (double*)enif_alloc(total_num_values * sizeof(double));
    size_t num_ticks_per_bar_index = 0;

    // Creating another array to keep track of buy_ticks_proportion, and index to keep track of size
    double* buy_ticks_proportion = (double*)enif_alloc(total_num_values * sizeof(double));
    size_t buy_ticks_proportion_index = 0;

    // Quick check all memory was allocated alright
    if (num_ticks_per_bar == NULL || buy_ticks_proportion == NULL || imbalance_array_buy == NULL || imbalance_array_sell == NULL) {
        enif_free(num_ticks_per_bar);
        enif_free(buy_ticks_proportion);
        enif_free(imbalance_array_buy);
        enif_free(imbalance_array_sell);
        return enif_make_badarg(env);
    }

    // Our expected values we update every cycle and initialization values for the 'warmup' period
    double expected_buy_ticks_proportion = 0.0;
    double expected_imbalance_buy = 0.0;
    double expected_imbalance_sell = 0.0;
    u_int8_t expected_imbalance_buy_initialized = 0;
    u_int8_t expected_imbalance_sell_initialized = 0;
    u_int8_t expected_imbalance_initialized = 0;



    for (unsigned int i = 0; i < iovec->iovcnt; ++i)
    {
        SysIOVec* sysiov = &iovec->iov[i];
        size_t len = sysiov->iov_len;
        size_t num_values_in_bin = len / sizeof(double);
        const double* data = (const double*)sysiov->iov_base;

        for (size_t j = 0; j < num_values_in_bin; ++j, ++global_index)
        {
            double imbalance = data[j];

            out_data[global_index] = bar_number;
            ++cum_ticks;

            // Processing the imbalance correctly according to the direction (buy/sell) but using absolute values
            if (imbalance > 0) {
                cum_theta_buy += imbalance;
                imbalance_array_buy[imbalance_array_buy_index] = imbalance;
                ++imbalance_array_buy_index;
                ++buy_ticks_num;
            } else if (imbalance < 0) {
                cum_theta_sell += fabs(imbalance);
                imbalance_array_sell[imbalance_array_sell_index] = fabs(imbalance);
                ++imbalance_array_sell_index;
            }

            // Just like before, waiting for the 'warmup' period to complete and all our important values to get initialized
            if (bar_number == 0 && !expected_imbalance_initialized) {
                if (imbalance_array_buy_index >= expected_num_ticks) {
                    expected_imbalance_buy = get_expected_imbalance(imbalance_array_buy, imbalance_array_buy_index, expected_imbalance_window);
                    expected_imbalance_buy_initialized = 1;
                }
                
                if (imbalance_array_sell_index >= expected_num_ticks) {
                    expected_imbalance_sell = get_expected_imbalance(imbalance_array_sell, imbalance_array_sell_index, expected_imbalance_window);
                    expected_imbalance_sell_initialized = 1;
                }
                
                // This time, once we are done with the 'warmup', we also calculate the proportion of buy_ticks out of total cum_ticks for the first time
                if (expected_imbalance_buy_initialized && expected_imbalance_sell_initialized) {
                    expected_buy_ticks_proportion = (double)buy_ticks_num / cum_ticks;
                    expected_imbalance_initialized = 1;
                }
            } 

            if (!expected_imbalance_initialized) continue; // If still in 'warmup' continue to next cycle

            // After 'warmup', we get the max value between buy_ticks_proportion and "sell_ticks_proportion" (1 - expected_buy_ticks_proportion)
            double max_proportion = fmax(expected_imbalance_buy * expected_buy_ticks_proportion, expected_imbalance_sell * (1 - expected_buy_ticks_proportion));
            // And choose the max cumulative imbalance out of the two
            double max_theta = fmax(cum_theta_buy, cum_theta_sell);

            // Creating NEW BAR if our threshold reached
            if (max_theta > expected_num_ticks * max_proportion) {
                // This part is the same as in copmute_imbalance_bars 
                if (ema_bars) {
                    num_ticks_per_bar[num_ticks_per_bar_index] = (double)cum_ticks;
                    ++num_ticks_per_bar_index;

                    size_t start = (num_ticks_per_bar_index >= num_prev_bars) ? num_ticks_per_bar_index - num_prev_bars : 0;
                    size_t size = num_ticks_per_bar_index - start;

                    double *slice = &num_ticks_per_bar[start];
                    double ewma_value = calculate_ewma(slice, size, num_prev_bars_alpha);
                    expected_num_ticks = fmin(expected_num_ticks_constraints_max, fmax(ewma_value, expected_num_ticks_constraints_min));
                }

                // We update expected_imbalance separately for buy and sell, as opposed to only once in copmute_imbalance_bars
                expected_imbalance_buy = get_expected_imbalance(imbalance_array_buy, imbalance_array_buy_index, expected_imbalance_window);
                expected_imbalance_sell = get_expected_imbalance(imbalance_array_sell, imbalance_array_sell_index, expected_imbalance_window);

                // This time we also update our buy_ticks_proportion array, and with it calculate the new expected_buy_ticks_proportion
                buy_ticks_proportion[buy_ticks_proportion_index] = (double)buy_ticks_num / cum_ticks;
                ++buy_ticks_proportion_index;
                size_t start = (buy_ticks_proportion_index >= num_prev_bars) ? buy_ticks_proportion_index - num_prev_bars : 0;
                size_t size = buy_ticks_proportion_index - start;
                double *slice = &buy_ticks_proportion[start];
                expected_buy_ticks_proportion = calculate_ewma(slice, size, num_prev_bars_alpha);

                // Reset all our cumulative variables
                cum_theta_buy = 0.0;
                cum_theta_sell = 0.0;
                buy_ticks_num = 0;
                cum_ticks = 0;
                ++bar_number; // And increment Bar Number to reflect the new bar
            }

        }
    }

    // Free allocated memory
    enif_free(num_ticks_per_bar);
    enif_free(buy_ticks_proportion);
    enif_free(imbalance_array_buy);
    enif_free(imbalance_array_sell);
    
    // And return our result
    return result;
}
