import sys
# python文件如何接受外界参数
if __name__ == '__main__':
    x=sys.argv[1]
    y=sys.argv[2]
    z=int(x)+int(y)
    ret='两数之和='+str(z)
    print(ret)
