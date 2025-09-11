combined_list = [2, 5, [43, [34, 11]], 56, 24, [1, 4, 6, [7, 8]], 0]


def flattern_list(nested_list):
    flatternList = []
    for each_element in nested_list:
        if type(each_element) == list:
            flatternList.extend(flattern_list(each_element))
        else:
            flatternList.append(each_element)
    return flatternList

print(flattern_list(combined_list))
