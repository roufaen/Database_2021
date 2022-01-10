# 项目说明

本项目为清华大学 2021 年数据库系统概论大作业。

作者： [@robinren03](https://github.com/robinren03) ； [@roufaen](https://github.com/roufaen) 。

# 项目运行方法

本项目在 Linux 操作系统下开发和运行，环境依赖为 `cmake` （实验中版本为 `3.5.1` ）。

在项目目录下依次运行如下 bash 命令。

```sh
mkdir build   # build directory
cd build
cmake ..
make
cd src        # executive name is database2021
```

此时， database2021 为可执行文件。

在项目目录下新建 data 目录，并在其下放置验收数据，然后运行可执行文件并输入 sql 目录下的 create.sql 中的指令即可导入数据。
