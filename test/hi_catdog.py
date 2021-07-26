# encoding: utf-8
# author TurboChang


def Hi():
    content = input("你想跟我说什么呢？")
    str_1 = content.replace("猫", "#")
    str_2 = str_1.replace("狗", "猫")
    str_3 = str_2.replace("#", "狗")
    print(str_3)

if __name__ == '__main__':
    Hi()
