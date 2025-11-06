#!/bin/bash
# WSL Setup Script for Data Ingestion Pipeline

echo "🚀 Setting up Data Ingestion Pipeline on WSL..."

# Update system packages
echo "📦 Updating system packages..."
sudo apt update && sudo apt upgrade -y

# Install Java (required for PySpark)
echo "☕ Installing Java 11..."
sudo apt install -y openjdk-11-jdk

# Set JAVA_HOME
echo "🔧 Setting JAVA_HOME..."
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/.bashrc
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$PATH:$JAVA_HOME/bin

# Install Python and pip
echo "🐍 Installing Python dependencies..."
sudo apt install -y python3 python3-pip python3-venv

# Install Docker (for MongoDB and Kafka)
echo "🐳 Installing Docker..."
sudo apt install -y apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io

# Add user to docker group
sudo usermod -aG docker $USER

# Install Docker Compose
echo "🐳 Installing Docker Compose..."
sudo curl -L "https://github.com/docker/compose/releases/download/v2.20.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Create directories
echo "📁 Creating directories..."
mkdir -p logs data

# Set permissions
chmod +x setup_wsl.sh

echo "✅ WSL setup completed!"
echo "📝 Please run 'source ~/.bashrc' or restart your terminal to apply environment changes."
echo "🔄 You may need to restart WSL for Docker to work properly."