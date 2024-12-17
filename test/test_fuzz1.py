from rapidfuzz import fuzz, process, utils
choices = ["new","nuovo","now"]
print(process.extract("NEW",choices, scorer=fuzz.ratio, processor=utils.default_process))
print(fuzz.ratio("new","New"))
print(fuzz.ratio("new","NEW",processor=utils.default_process))
print(fuzz.ratio("John","Jhon",processor=utils.default_process))
print(fuzz.ratio("Daikin","DAIKIN PTY LTD",processor=utils.default_process))
print(int(fuzz.ratio("Mitsubishi","MITSUBISHI PTY LTD",processor=utils.default_process)))
print(int(fuzz.ratio("aaaaaa","zzzzzz",processor=utils.default_process)))
#fuzzies = ['"{}"'.format(x[0]) for x in extracted if x[1]>85]
#raise ValueError ('could not find column {}. Did you mean {}?'.format(column,' or '.join(fuzzies)))
#suggested_columns = self._suggest_values(column, self.get_columns())

#AttributeError: 'Table' object has no attribute '_suggest_values'. Did you mean: '_suggest_columns'?

# extracted = process.extract(column, self.get_columns(), processor=utils.default_process)
# fuzzies = [repr(x[0]) for x in extracted if x[1] > 75]
# raise ValueError ('could not find column {}. Did you mean {}?'.format(repr(column),' or '.join(fuzzies)))