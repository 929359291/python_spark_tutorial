def numGenerator():
    for x in [1, 2, 3, 4, 5]:
        yield x


if __name__ == '__main__':
    """
    yield用来生成迭代器，即组成一个对象
    """
    print(type(numGenerator()))
    for n in numGenerator():
        print(n)
