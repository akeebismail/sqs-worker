<?php


use Aws\Sqs\SqsClient;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\EventLoop\TimerInterface;

class Worker
{
    private LoopInterface $loop;
    private $checkForMessage = 1;
    public function __construct(
        private SqsClient $sqsClient,
        private string $queueUrl,
        private int $maxNumberOfMessages,
        private int $waitTimeSeconds,
        private int $visibilityTimeout,
    ) {
        $this->loop = Loop::get();
    }
    public function convertMessage(string $message): array
    {
        return json_decode($message, true);
    }
    public function consume(callable $handler)
    {
        $worker = $this->loop->addPeriodicTimer($this->checkForMessage, function (TimerInterface $timer) use($handler) {
            $this->getMessages($timer,function ($messages) use ($handler) {
                foreach ($messages as $message) {
                    $done = $handler($this->convertMessage($message));
                    if ($done) {
                        $this->ackMessage($message);
                    } else {
                        $this->nackMessage($message);
                    }
                }
            });
        });


    }

    private function getMessages(TimerInterface $timer, $handler)
    {
        $result = $this->sqsClient->receiveMessage([
            'AttributeNames'        => ['SentTimestamp'],
            'MaxNumberOfMessages'   => $this->maxNumberOfMessages,
            'MessageAttributeNames' => ['All'],
            'QueueUrl'              => $this->queueUrl, // REQUIRED
            'WaitTimeSeconds'       => $this->waitTimeSeconds,
            'VisibilityTimeout'     => $this->visibilityTimeout,
        ]);
        $messages = $result->get('Messages');
        if ($messages != null) {
            $handler($messages);
            $this->checkForMessage = 1;
        } else {
            $this->checkForMessage = 5;
        }
    }

    /**
     * Ack message
     *
     * @param $message
     * @throws Exception
     */
    private function ackMessage($message)
    {
        if ($this->sqsClient == null) {
            throw new Exception("No SQS client defined");
        }

        $this->sqsClient->deleteMessage([
            'QueueUrl'      => $this->queueUrl, // REQUIRED
            'ReceiptHandle' => $message['ReceiptHandle'], // REQUIRED
        ]);
    }

    /**
     * Nack message
     *
     * @param $message
     * @throws Exception
     */
    private function nackMessage($message)
    {
        if ($this->sqsClient == null) {
            throw new Exception("No SQS client defined");
        }

        $this->sqsClient->changeMessageVisibility([
            // VisibilityTimeout is required
            'VisibilityTimeout' => 0,
            'QueueUrl'          => $this->queueUrl, // REQUIRED
            'ReceiptHandle'     => $message['ReceiptHandle'], // REQUIRED
        ]);
    }
}