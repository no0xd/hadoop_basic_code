1.mr      wc        单词计数
2.mr2     sort      排序
3.mr3     combiner  单词计数 如无特殊情况 归约可直接使用Reducer类 如有定制化 则继承reducer即可
4.mr4     count     流量统计 sum使用时,Long需要有初始值 否则为null 执行时会报空指针
5.mr5     count     流量统计后按下行流量总数排序,取前十
  5.1 v3竟然不能为 NullWritable
  5.2 k3设置为空时 不能归约

6.mr6      count     流量统计后,按手机号进行分区



