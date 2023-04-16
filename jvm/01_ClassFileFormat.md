# JVM

## 1：JVM基础知识

1. 什么是JVM

   JVM是Java Virtual Machine（Java[虚拟机](https://baike.baidu.com/item/虚拟机)）的缩写，jvm能运行多种语言，只要能编译成class文件，jvm就能运行

   	1. jvm是一个跨语言的平台
    	2. JVM是一种用于计算设备的规范
    	3. jvm是虚拟出来的计算机

2. 常见的JVM

   Hotspot   oracle官方，我们做实验用的JVM

   Jrockit  BEA，曾经号称世界上最快的JVM，被Oracle收购，合并于hotspot

   J9 - IBM

   Microsoft  VM

   TabaoVM   hotspot深度定制版

   Liquid VM  直接针对硬件

   azul zing  最新垃圾回收的业界标杆 www.azul.com

   

   ![image-20200606182127302](.\images\image-20200606182127302.png)

## 2：ClassFileFormat

1. class文件是二进制字节流

2. 数据类型： u1 u2 u4 u8和_info（表类型）

   _info的来源是hotspot源码中的写法 

3. 查看16进制格式的ClassFile

   sublime   notepad

   IDEA插件   BinEd

4. 有很多可以观察ByteCode的方法

   javap   	 javap -v 路径 查看class文件详细信息

   JBE  可以直接修改

   jClassLib  - IDEA插件之一

5. classfile构成    详见马士兵教育 java1.8类文件格式第一版.png图片

   常量池从1开始，0是jvm预留的，表示没有任何引用指向

   

## 3：类编译-加载-初始化

hashcode
锁的信息（2位 四种组合）
GC信息（年龄）
如果是数组，数组的长度

## 4：JMM

new Cat()
pointer -> Cat.class
寻找方法的信息

## 5：对象

1：句柄池 （指针池）间接指针，节省内存
2：直接指针，访问速度快

## 6：GC基础知识

栈上分配
TLAB（Thread Local Allocation Buffer）
Old
Eden
老不死 - > Old

## 7：GC常用垃圾回收器

new Object()
markword          8个字节
类型指针           8个字节
实例变量           0
补齐                  0		
16字节（压缩 非压缩）
Object o
8个字节 
JVM参数指定压缩或非压缩

