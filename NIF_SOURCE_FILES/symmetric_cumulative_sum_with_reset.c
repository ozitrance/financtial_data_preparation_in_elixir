// Including the Erlang NIF header file to populate our file with all the relevant functions
#include <erl_nif.h>
#include <math.h>
// Creating our function starting with the result type (ERL_NIF_TERM), and the arguments: the Erlang enviorment, argument count, and the arguments array
ERL_NIF_TERM symmetric_cumulative_sum_with_reset(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {

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

    // Calculating the number of values according to their size (sizeof double in this case)
    size_t total_num_values = iovec->size / sizeof(double); // And calculating the number of values according to their size (sizeof double in this case)

    // Allocate the output binary so we can write to it in 1 pass while we process the input data
    ErlNifBinary out_binary;
    if (!enif_alloc_binary(total_num_values * sizeof(int64_t), &out_binary)) {
        return enif_make_badarg(env);
    }
    int64_t* out_data = (int64_t*)out_binary.data; // Casting our data to int64 - our timestamp index / simple integers
    size_t out_index = 0;

    // Initial Values
    double cumulative_sum_positive = 0.0;
    double cumulative_sum_negative = 0.0;

    // We can't use the main loop index because the data might span over few SysIOVec's (data bins/structs) so we start our own counter
    size_t global_index = 0; 

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
        for (size_t j = 0; j < num_values_in_bin; ++j, ++global_index)
        {
            double val = data[j];   // Current Value

            cumulative_sum_positive = fmax(0.0, cumulative_sum_positive + val); // Updating the positive cumulative value, keeping it above 0.0
            cumulative_sum_negative = fmin(0.0, cumulative_sum_negative + val); // Updating the negative cumulative value, keeping it below 0.0

            // Check for threshold crossing and if any crosses, create event (add global_index to output) and reset associated cumulative counter
            if (cumulative_sum_negative < -threshold) {
                out_data[out_index++] = global_index;
                cumulative_sum_negative = 0.0;
            } else if (cumulative_sum_positive > threshold) {
                out_data[out_index++] = global_index;
                cumulative_sum_positive = 0.0;
            }
        }
    }

    // Resize the output binary to the actual number of indices
    size_t out_size = out_index * sizeof(int64_t);
    if (!enif_realloc_binary(&out_binary, out_size)) {
        // Cleanup in case of failure
        enif_release_binary(&out_binary);
        return enif_make_badarg(env);
    }


    // Create the Erlang binary term and return
    ERL_NIF_TERM result = enif_make_binary(env, &out_binary);
    return result;
}
