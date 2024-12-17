class Parent(object):
     def __init__(self):
         self.value = 4
     def get_value(self):
         return self.value
 
class Child(Parent):
    #pass
    def __init__(self):
         self.value = 2
    def get_value(self):
        return self.value + 1


c = Child()
# print(c.get_value())

print(Parent.__dict__)
print('\n',Child.__dict__)

print(c.get_value())