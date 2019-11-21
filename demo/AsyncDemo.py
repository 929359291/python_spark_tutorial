#!/usr/bin/env python3
# AsyncDemo.py
import asyncio
import time


async def count():
    print("One")
    await asyncio.sleep(1)
    print("Two")


async def main():
    await asyncio.gather(count(), count(), count())


def count2():
    print("One")
    time.sleep(1)
    print("Two")


def main2():
    for _ in range(3):
        count2()


if __name__ == '__main__':
    # asyncio.run(main())
    main2()
