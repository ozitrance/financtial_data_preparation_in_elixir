// Including the Erlang NIF header file to populate our file with all the relevant functions
#include <erl_nif.h>

// Creating our function starting with the result type (ERL_NIF_TERM), and the arguments: the Erlang enviorment, argument count, and the arguments array
ERL_NIF_TERM cumulative_sum_with_reset(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {

    // Quick check to make sure we got the amount of arguments were expecting
    if (argc != 2) return enif_make_badarg(env);

    // Getting our threshold value    
    double threshold;
    if (!enif_get_double(env, argv[1], &threshold)) {
        return enif_make_badarg(env);
    }

    // Getting the values 
    // Starting with the iovec_term value out of the arguments array
    ERL_NIF_TERM iovec_term = argv[0];
    
    // Creating iovec pointer to hold the values
    ErlNifIOVec *iovec = NULL;

    // In the enif_inspect_iovec function, if we choose to limit the items (2nd argument), the rest will be stored in the tail
    // Since we don't set a limit (0), this will hold nothing
    ERL_NIF_TERM tail;

    // Inspect the iovec and get the data out
    if (!enif_inspect_iovec(env, 0, iovec_term, &tail, &iovec))
    {
        return enif_make_badarg(env);
    }

    // Estimating needed size for our output buffer so we can fill it up while looping through the data, all in one pass
    size_t total_input_bytes = iovec->size;
    size_t total_num_values = total_input_bytes / sizeof(double); // And calculating the number of values according to their size (sizeof double in this case)

    // Allocate the output binary so we can write to it in 1 pass while we process the input data
    ERL_NIF_TERM result;
    unsigned char* out_data_raw = enif_make_new_binary(env, total_num_values * sizeof(int64_t), &result);
    int64_t* out_data = (int64_t*)out_data_raw; // Casting our data to int64 - our bar numbers / simple integers

    // Initial Values
    double cum_value = 0.0;
    int64_t bar_number = 1;
    // We can't use the main loop index because the data might span over few SysIOVec's (data bins/structs) so we start our own counter
    size_t out_index = 0;

    // Iterate over each SysIOVec in the iovec, until we reach max elements: iovec->iovcnt
    for (unsigned int i = 0; i < iovec->iovcnt; ++i)
    {   
        // Getting the current SysIOVec struct
        SysIOVec* sysiov = &iovec->iov[i];
        // Getting the binary length
        size_t len = sysiov->iov_len;

        // Calculating number of values given we have fixed-length doubles only
        size_t num_values_in_bin = len / sizeof(double);
        // Getting the data from the struct
        const double* data = (const double*)sysiov->iov_base;

        // Looping over the data with known number of values
        for (size_t j = 0; j < num_values_in_bin; ++j)
        {
            double val = data[j];   // Current Value

            out_data[out_index++] = bar_number; // Adding the current bar to our output bars array
            cum_value += val; // Updating the cumulative value

            // If new_cum_value is greater than our threshold we reset our cum_value and increment the current bar_number
            if (cum_value >= threshold)
            {
                cum_value = 0.0;
                ++bar_number;
            }
        }
    }

    // And we just need to return our result 
    return result;
}
