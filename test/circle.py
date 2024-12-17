import math

class Circle(object):
    'Ad adanced circle'

    def __init__(self, radius):
        self.radius = radius

    def area(self):
        'perform quadrature on a shape of uniform radius'
        print(math.pi)
        return math.pi * self.radius ** 2.0


c = Circle(2)
print(c.area())