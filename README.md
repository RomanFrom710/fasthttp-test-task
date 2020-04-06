# fasthttp-test-task

Tool for testing: https://github.com/RomanFrom710/fasthttp-test-task-load-tester

## Installation

- Save your AWS credentials in the home directory (see [AWS docs](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/setting-up.html) for more information)
- Replace `region` and `bucket` constants in `amazon.go` with your data

## QPS and memory usage

The average QPS value is around `2000`. I used simple tool that I wrote to send requests with random data and measure QPS. It's possible to retrieve better results if use static mock data and well-optimized benchmark framework.

Most of the memory is taken by buffer arrays for storing data to be sent to AWS, so the memory usage is about 150MB. It's suitable for 10 clients, but in case their amount grows it's better to consider other option, such as Redis for example.

## Possible ways to improve

- Use optimized json parser (for example [fastjson](https://github.com/valyala/fastjson))
- Use better algorithm to determine when gzip'ed data chunk exceeds 5 MiB

## Spent time

Five 6-hours days were spent (i.e. 30 hours)
