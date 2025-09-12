
def is_palindrome(input_str):
    i = 0
    j = len(input_str) - 1
    while i < j:
        if input_str[i] != input_str[j]:
            return False
        i += 1
        j -= 1
    return True

def make_it_a_palindrome(input_str):
    cnt = 0
    if is_palindrome(input_str):
        print("Palindrome String:- ", input_str)
        print("No of letters added to make it a palindrome:- ", cnt)
        return
    n = len(input_str) - 1
    prefix_string = ''
    while n >= 0:
        prefix_string += input_str[n]
        cnt += 1
        output = prefix_string + input_str
        if is_palindrome(output):
            print("Palindrome String:- ", output)
            print("No of letters added to make it a palindrome:- ", cnt)
            return
        n -= 1

input_str = input("Enter a string: ")
make_it_a_palindrome(input_str)
