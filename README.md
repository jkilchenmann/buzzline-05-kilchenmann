# buzzline-05-kilchenmann

Nearly every streaming analytics system stores processed data somewhere for further analysis, historical reference, or integration with BI tools.

In this example project, we incorporate a relational data store. 
We use SQLite, but the example could be altered to work with MySQL, PostgreSQL, or MongoDB.

## VS Code Extensions

- Black Formatter by Microsoft
- Markdown All in One by Yu Zhang
- PowerShell by Microsoft (on Windows Machines)
- Pylance by Microsoft
- Python by Microsoft
- Python Debugger by Microsoft
- Ruff by Astral Software (Linter)
- **SQLite Viewer by Florian Klampfer**
- WSL by Microsoft (on Windows Machines)

## Task 1. Use Tools from Module 1 and 2

Before starting, ensure you have completed the setup tasks in <https://github.com/denisecase/buzzline-01-case> and <https://github.com/denisecase/buzzline-02-case> first. 

Versions matter. Python 3.11 is required. See the instructions for the required Java JDK and more. 

## Task 2. Copy This Example Project and Rename

Once the tools are installed, copy/fork this project into your GitHub account
and create your own version of this project to run and experiment with. 
Follow the instructions in [FORK-THIS-REPO.md](https://github.com/denisecase/buzzline-01-case/docs/FORK-THIS-REPO.md).

OR: For more practice, add these example scripts or features to your earlier project. 
You'll want to check requirements.txt, .env, and the consumers, producers, and util folders. 
Use your README.md to record your workflow and commands. 
    

## Task 3. Manage Local Project Virtual Environment

Follow the instructions in [MANAGE-VENV.md](https://github.com/denisecase/buzzline-01-case/docs/MANAGE-VENV.md) to:
1. Create your .venv
2. Activate .venv
3. Install the required dependencies using requirements.txt.

## Task 4. Start Zookeeper and Kafka (Takes 2 Terminals)

If Zookeeper and Kafka are not already running, you'll need to restart them.
See instructions at [SETUP-KAFKA.md] to:

1. Start Zookeeper Service ([link](https://github.com/denisecase/buzzline-02-case/blob/main/docs/SETUP-KAFKA.md#step-7-start-zookeeper-service-terminal-1))
2. Start Kafka Service ([link](https://github.com/denisecase/buzzline-02-case/blob/main/docs/SETUP-KAFKA.md#step-8-start-kafka-terminal-2))

---

## Task 5. Start a New Streaming Application

This will take two more terminals:

1. One to run the producer which writes messages. 
2. Another to run the consumer which reads messages, processes them, and writes them to a data store. 

### Producer (Terminal 3) 

Start the producer to generate the messages. 

If Zookeeper and Kafka services are running, it will try to write them to a Kafka topic as well.
For configuration details, see the .env file. 

In VS Code, open a NEW terminal.
Use the commands below to activate .venv, and start the producer. 

Windows:

```shell
.venv\Scripts\activate
py -m producers.producer_case
```

Mac/Linux:
```zsh
source .venv/bin/activate
python3 -m producers.producer_case
```

The producer will still work if Kafka is not available.

### Consumer (Terminal 4)

Start an associated consumer. 

In VS Code, open a NEW terminal in your root project folder. 
Use the commands below to activate .venv, and start the consumer. 

Windows:
```shell
.venv\Scripts\activate
py -m consumers.consumer_kilchenmann

```

Mac/Linux:
```zsh
source .venv/bin/activate
python3 -m consumers.consumer_kilchenmann

```

---

## What the Consumer Does

The custom consumer looks for messages that include “Python” as a topic.  When a message with “Python” as a topic is received, 
the message is written to a CSV file that contains the message and the author.  
A bar chart tracks how many times each author mentions Python.  This could be a good way to only display alerts for important issues, 
rather than having to sift through every message to see when something imporant comes up.

## License
This project is licensed under the MIT License as an example project. 
You are encouraged to fork, copy, explore, and modify the code as you like. 
See the [LICENSE](LICENSE.txt) file for more.
