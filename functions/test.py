def str_manip(a,b):
	result = ''
	for i, char in enumerate(a):
		if i % 2 == 0:
			result += a[i]
		else:
			result += b[i]
	return result

print(str_manip('12345', '67890'))