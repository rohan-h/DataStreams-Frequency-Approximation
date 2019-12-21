import numpy as np
import math

def estimate(dataset_path, counts_path, hash_path, delta, epsilon, p):
    def hash_func(a, b, p, buckets, x):
        y = x % p
        hash_val = (a * y + b) % p
        return hash_val % buckets

    def count(x):
        count = []
        for i, params in enumerate(hash_params):
            hash_val = hash_func(params[0], params[1], p, e_by_epsilon, x)
            count.append(hash_matrix[i][hash_val])
        return count

    def error_func(fx, fy):
        error = (fx - fy) / fy
        return error

    hash_params = np.genfromtxt(hash_path, dtype=np.int32)
    num_hash_func = int(math.log(1/delta))
    count_params = np.genfromtxt(counts_path, dtype=np.int32)
    words_stream = np.genfromtxt(dataset_path, dtype=np.int32)

    hash_matrix = np.zeros(shape=(num_hash_func, e_by_epsilon), dtype=np.int32)

    print("calculating hash matrix")
    for x in words_stream:
        for i, params in enumerate(hash_params):
            hash_val = hash_func(params[0], params[1], p, e_by_epsilon, x)
            hash_matrix[i][hash_val] += 1

    print("counting data stream")
    words_list_count = []
    for x in np.unique(words_stream):
        hash_count = count(x)
        min_count = min(hash_count)
        words_list_count.append([x, min_count])

    np.savetxt('output_count.txt', words_list_count,  delimiter='\t', fmt="%s")

    print("counting error")
    error_list = []
    for fx, fy in zip(words_list_count, count_params):
        if fx[0] == fy[0]:
            error = error_func(fx[1], fy[1])
            error_list.append([fx[0], error])
        else:
            print("values are different")

    np.savetxt('error_count.txt', error_list,  delimiter='\t', fmt="%s")


dataset_path = 'words_stream.txt'
counts_path = 'counts.txt'
hash_path = 'hash_params.txt'

e = math.exp(1)
p = 123457
delta = pow(e, -5)
epsilon = e * pow(10, -4)
e_by_epsilon = math.ceil(e / epsilon)
estimate(dataset_path, counts_path, hash_path, delta, epsilon, p)
