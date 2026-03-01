def print_rangoli(size):
    pattern = []
    final_pattern = []
    for i in range(0,size):
        for k in range(i, size):
            value = chr(97+k)
            if pattern == []:
                pattern.append(value)
            else:
                pattern.append(value)
                pattern.insert(0,value)
            j = k + 1
            if j < size: 
                pattern.insert(0,'-')
                pattern.append('-')
        if i == 0:
            len_val = len(pattern)
        final_pattern.append(''.join(pattern))
        pattern = []
  
    for rev_each_pattern in range(len(final_pattern),1,-1):
        val = final_pattern[rev_each_pattern-1]
        print(val.center(len_val,'-'))
    for each_pattern in final_pattern:
        print(each_pattern.center(len_val,'-'))

if __name__ == '__main__':
    n = int(input())
    print_rangoli(n)
