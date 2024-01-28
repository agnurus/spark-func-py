
#Functional programming is a paradigm that treats computation as the evaluation of mathematical functions. Here are some core concepts

# Example of immutability
immutable_list = (1, 2, 3)
# immutable_list[0] = 4  # This will raise an error

# Pure Functions: Functions that produce the same output for the same input and have no side effects.
def add(x, y): return x + y
result = add(3, 4)  # Always produces 7
def multiply_values(d): return {k: v * 2 for k, v in d.items()}



#Higher-order Functions: Functions that can take other functions as arguments or return functions.
def square(x, y): return x * y
def peration(operation, x, y):
    return operation(x, y)
def apply_operation(operation, x, y): return operation(x, y)
def apply_func_to_values(func, d): return {k: func(v) for k, v in d.items()}
result = apply_operation(square, 3, 4)

#Anonymous Functions: Functions without a name
anonymous_func = lambda x: x * 2

# Example of a lambda expression
multiply = lambda x, y: x * y
result = multiply(3, 4)  # Returns 12
print("multiply-",multiply)
print("result-",result)