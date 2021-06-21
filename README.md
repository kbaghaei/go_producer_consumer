# How Go Helped Me Accelerate My Machine Learning Calculations?

**Summary of the project**:

In this project, I used Go language in order to run a pool of machine learning processes. Rather than implementing the logic of multi-processing that can utilize the hardware resouces to its finest, I chose to have the OS take care of that stuff. In Go, in an async loop, I assign small parts of the AI task to a pool of individual external processes that run concurrently on different CPU cores.

Please refer to the blog corresponding to this github project [available here](https://pub.towardsai.net/how-go-helped-me-accelerate-my-machine-learning-computations-b2ea961130ad) and feel free to reach out to me with your feedbacks.
