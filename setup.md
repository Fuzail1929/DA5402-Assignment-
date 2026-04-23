# Environment Setup

## System Requirements
- macOS (Apple Silicon M1/M2 or Intel)
- Homebrew 5.1.7
- Java OpenJDK 11

## Step 1 - Install Homebrew
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

## Step 2 - Install Java
```bash
brew install openjdk@11
echo 'export PATH="/opt/homebrew/opt/openjdk@11/bin:$PATH"' >> ~/.zshrc
echo 'export JAVA_HOME="/opt/homebrew/opt/openjdk@11"' >> ~/.zshrc
source ~/.zshrc
java -version
```

## Step 3 - Install Python Packages
```bash
pip3 install -r requirements.txt
```

## Verify Installation
```bash
java -version
python3 -c "import pyspark; print('PySpark:', pyspark.__version__)"
python3 -c "import ray; print('Ray:', ray.__version__)"
python3 -c "import pandas; print('Pandas:', pandas.__version__)"
python3 -c "import pyarrow; print('PyArrow:', pyarrow.__version__)"
python3 -c "import psutil; print('Psutil:', psutil.__version__)"
```

## Expected Output
- Homebrew: 5.1.7
- Java: openjdk 11.x.x
- PySpark: 4.0.0
- Ray: 2.51.2
- PyArrow: 20.0.0
- Pandas: 2.3.1
- Psutil: 5.9.8
