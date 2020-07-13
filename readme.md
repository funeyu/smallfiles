## 模块设计思想
将文件file 分成一个个固定1mb大小的区块，每次加载到内存的时候都是以1mb的大小直接load到内存中；

爬虫爬取的内容，以域名为单位去存储到每个1mb区块中，然后记录相应的大小，偏移等meta信息；
当然每个域名的数据存储也会大于1mb，此时再加个1mb区块，并在之前的区块中设置相应的next指针指向新建的区块；

一个区块最多保存一个域名的存储数据，这样子会造成部分的磁盘空间浪费，但这是为了减少随机读取而进行的优化；

整个smallfiles系统有多个file，读写可以分别在不同file上，保证一定的并发度；