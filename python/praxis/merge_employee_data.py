# Example Usage:
list1 = [
    {'id': 101, 'name': 'John', 'department': 'Sales'},
    {'id': 102, 'name': 'Jane', 'department': 'HR'}
]
list2 = [
    {'id': 102, 'name': 'Jane Doe', 'department': 'Marketing'},
    {'id': 103, 'name': 'Peter', 'department': 'IT'}
]

def merge_employee_data(list1, list2):
    merged_data = {}
    for record in list1:
        merged_data[record['id']] = record
    for record in list2:
        merged_data[record['id']] = record
    final_list = []
    print(merged_data)
    for key in merged_data:
        final_list.append(merged_data[key])
    return final_list

print(merge_employee_data(list1, list2))

