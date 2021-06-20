import math


def data_retrieval(chunk_size, total_size, database):
    """
    Retrieves from MongoDB Atlas a given collection, in predefined chunk sizes.
    :param chunk_size: number of documents in each chunk
    :param total_size: number of documents in the given collection
    :param database: address of the retrieved collection
    :return: list of retrieved chunks
    """
    chunk_stack = list()
    current_indices = [0, chunk_size + 1]

    for i in range(0, int(total_size / chunk_size)):
        data_batch = database.aggregate(
            [
                {
                    "$match": {
                        "index": {
                            "$gt": current_indices[0], "$lt": current_indices[1]
                        }
                    }
                }
            ]
        )

        current_batch = [x for x in data_batch]
        chunk_stack.append(current_batch)
        current_indices = [x + chunk_size for x in current_indices]
    return chunk_stack


def k_way_merge_sort(list_of_lists):
    """
    Implementation of k-way merge sort algorithm
    :param list_of_lists: a list which contains unsorted arrays
    :return: a single sorted arrays
    """
    k_size = len(list_of_lists)
    sorted_list_of_lists = list()

    for chunk in list_of_lists:
        clean_data = extract_value_by_key("data", chunk)
        clean_data.sort()
        sorted_list_of_lists.append(clean_data)

    sorted_and_merged = merge(sorted_list_of_lists, k_size)
    return sorted_and_merged


def extract_value_by_key(key, list_of_dicts):
    """
    Discard any unneeded data that was retrieved from the database, and append the rest to a single list
    :param key: key of relevant data in each dictionary
    :param list_of_dicts: a list of dictionaries
    :return: a list of strings which was retrieved from the given dictionaries
    """
    list_of_values = [x[key] for x in list_of_dicts]
    return list_of_values


def merge(chunks_stack, k):
    """
    A high function of merge-sort, which applies the k-way algorithm via recursive call of a single merge function
    :param chunks_stack:
    :param k:
    :return:
    """
    if k == 1:
        return chunks_stack[0]
    if k == 2:
        return merge_arrays(chunks_stack[0], chunks_stack[1], len(chunks_stack[0]), len(chunks_stack[1]))

    else:
        half = math.floor(k / 2)

        sorted_array_1 = merge(chunks_stack[0: half], half)
        sorted_array_2 = merge(chunks_stack[half: k], k - half)

        return merge_arrays(sorted_array_1, sorted_array_2, len(sorted_array_1), len(sorted_array_2))


def merge_arrays(arr1, arr2, n1, n2):
    """
    Merge two sorted arrays to a single sorted array.
    :param arr1: 1st array
    :param arr2: 2nd array
    :param n1: length of 1st array
    :param n2: length of 2nd array
    :return: merged array
    """
    arr3 = [None] * (n1 + n2)
    i = 0
    j = 0
    k = 0

    # Traverse both arrays
    while i < n1 and j < n2:

        # Check if current element
        # of first array is smaller
        # than current element of
        # second array. If yes,
        # store first array element
        # and increment first array
        # index. Otherwise do same
        # with second array
        if arr1[i] < arr2[j]:
            arr3[k] = arr1[i]
            k = k + 1
            i = i + 1
        else:
            arr3[k] = arr2[j]
            k = k + 1
            j = j + 1

    # Store remaining elements
    # of first array
    while i < n1:
        arr3[k] = arr1[i]
        k = k + 1
        i = i + 1

    # Store remaining elements
    # of second array
    while j < n2:
        arr3[k] = arr2[j]
        k = k + 1
        j = j + 1
    return arr3

