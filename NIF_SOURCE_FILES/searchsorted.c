#include <erl_nif.h>

ERL_NIF_TERM searchsorted(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {

    if (argc != 3) return enif_make_badarg(env);

    // Getting our offset value    
    int offset;
    if (!enif_get_int(env, argv[1], &offset)) {
        return enif_make_badarg(env);
    }

    // Getting the values 
    // Second list (argv[2]) might be empty
    ERL_NIF_TERM iovec_term_A = argv[0];
    ERL_NIF_TERM iovec_term_B = argv[2];
    
    // Creating iovec pointer to hold the values
    ErlNifIOVec *iovec_A = NULL;
    ErlNifIOVec *iovec_B = NULL;

    // In the enif_inspect_iovec function, if we choose to limit the items (2nd argument), the rest will be stored in the tail
    // Since we don't set a limit (0), this will hold nothing
    ERL_NIF_TERM tail_A;
    ERL_NIF_TERM tail_B;
    // Inspect the iovec and get the data out
    if (!enif_inspect_iovec(env, 0, iovec_term_A, &tail_A, &iovec_A))
    {
        return enif_make_badarg(env);
    }

    // Checking if we have 2nd list to use as "haystack". Default is not (0)
    int two_lists = 0;

    // If the iovec_term_B (argv[2]) contains an iovec item we will use the 2nd list
    if (enif_inspect_iovec(env, 0, iovec_term_B, &tail_B, &iovec_B))
    {
        two_lists = 1;
    }

    size_t total_num_values_A = iovec_A->size / sizeof(int64_t); 
    
    // Calculating the number of values in the second list if we have two_lists, or using the number of the 1st list
    size_t total_num_values_B = two_lists ? iovec_B->size / sizeof(int64_t) : total_num_values_A; 

    ERL_NIF_TERM result;
    unsigned char* out_data_raw = enif_make_new_binary(env, total_num_values_A * sizeof(int64_t), &result);
    int64_t* out_data = (int64_t*)out_data_raw; 

    // Indexes for the "needles" (1st) list current iovec binary
    size_t i_bin_idx = 0;
    size_t i_bin_offset = 0;
    size_t i_bin_size = 0;
    const int64_t* i_data = NULL; // And preparing a variable to hold the data

    // Indexes for the "haystack" (2nd) list current iovec binary
    size_t j_bin_idx = 0;
    size_t j_bin_offset = 0;
    size_t j_bin_size = 0;
    const int64_t* j_data = NULL;

    // One more index to keep track of current index in loop for total_num_values_B (in the 2nd list - the "hasystack") 
    size_t j = 0; 

    // Initialize i pointers
    if (iovec_A->iovcnt > 0) {
        SysIOVec* iov_i = &iovec_A->iov[0];
        i_data = (const int64_t*)iov_i->iov_base;
        i_bin_size = iov_i->iov_len / sizeof(int64_t);
    }

    // Initialize j pointers - choosing the right one depending if we have two_lists or not
    if ((two_lists ? iovec_B : iovec_A)->iovcnt > 0) {
        SysIOVec* iov_j = &(two_lists ? iovec_B : iovec_A)->iov[0];
        j_data = (const int64_t*)iov_j->iov_base;
        j_bin_size = iov_j->iov_len / sizeof(int64_t);
    }

    // Starting to loop over our first list - the "needles"
    for (size_t i = 0; i < total_num_values_A; ++i) {
        // If we reached the end of the binary, we update pointers, counters, indexes 
        // and breaking the loop if there are no more binaries left
        if (i_bin_offset >= i_bin_size) {
            // Move to next binary for i
            ++i_bin_idx;
            i_bin_offset = 0;
            if (i_bin_idx < iovec_A->iovcnt) {
                SysIOVec* iov_i = &iovec_A->iov[i_bin_idx];
                i_data = (const int64_t*)iov_i->iov_base;
                i_bin_size = iov_i->iov_len / sizeof(int64_t);
            } else {
                break;
            }
        }
        // Getting the data from the 1st list for this cycle and setting the target
        int64_t data_i = i_data[i_bin_offset];
        int64_t target = data_i + offset;

        // Looking in the "haystack" list (the 2nd list)
        // Since lists are sorted we can use while and keep moving forward without resetting j and looking at the whole list every time
        while (j < total_num_values_B) {
            // As before, if we reached the end of the binary, we update pointers, counters, indexes 
            // and breaking the loop if there are no more binaries left
            if (j_bin_offset >= j_bin_size) {
                // Move to next binary for j
                ++j_bin_idx;
                j_bin_offset = 0;
                if (j_bin_idx < (two_lists ? iovec_B : iovec_A)->iovcnt) {
                    SysIOVec* iov_j = &(two_lists ? iovec_B : iovec_A)->iov[j_bin_idx];
                    j_data = (const int64_t*)iov_j->iov_base;
                    j_bin_size = iov_j->iov_len / sizeof(int64_t);
                } else {
                    break;
                }
            }
            
            // Getting the data for this cycle from the 2nd list
            int64_t data_j = j_data[j_bin_offset];
            
            // If we went over the target it means we found the closes item
            // so we break out of the while loop to save to our output
            if (data_j >= target) {
                break;
            }
            
            // Otherwise we update 2nd list indexes and keep searching with the while loop
            ++j;
            ++j_bin_offset;
        }
        
        // If we broke out of the while loop (or reached the end of our "haystack")
        // we add the index we found to the output list
        out_data[i] = j;

        // And updating the 1st list's iovec binary index (our "needles") so we can use the next item in the next loop
        // (our loop iterates over total number of items but iovec mighe spread all of them over unknown number of "sub lists")
        ++i_bin_offset;
    }

    // And we just need to return our result 
    return result;
}
