#!/usr/bin/env bash

# ===== Configuration =====
USER="sshuser"
WORKERS=("wn0-hibenc.2jfn0usmyiwevc0vch3k52ljsd.madx.internal.cloudapp.net" "wn1-hibenc.2jfn0usmyiwevc0vch3k52ljsd.madx.internal.cloudapp.net")

echo "=========================================="
echo "Generating SSH key if missing..."
echo "=========================================="

# ===== Generate SSH Key (if not exist) =====
if [ ! -f ~/.ssh/id_rsa ]; then
    ssh-keygen -q -N "" -t rsa -f ~/.ssh/id_rsa
else
    echo "SSH key already exists. Skipping generation."
fi

# ===== Show Public Key =====
echo "=========================================="
echo "Your Public Key (to be copied to workers):"
echo "=========================================="
cat ~/.ssh/id_rsa.pub
echo "=========================================="

# ===== Ask for SSH password once =====
echo "You will be prompted for password of $USER@each worker node."
read -s -p "Enter SSH Password: " PASSWORD
echo

# ===== Install sshpass if missing =====
if ! command -v sshpass &> /dev/null; then
    echo "Installing sshpass..."
    sudo apt-get update
    sudo apt-get install -y sshpass
fi

# ===== Copy SSH Public Key to Each Worker =====
for host in "${WORKERS[@]}"; do
    echo "------------------------------------------"
    echo "Copying SSH key to $host..."
    echo "------------------------------------------"
    
    sshpass -p "$PASSWORD" ssh-copy-id -o StrictHostKeyChecking=no "$USER@$host"
    
    echo "Testing SSH connection to $host..."
    ssh -o BatchMode=yes -o ConnectTimeout=5 "$USER@$host" 'echo "SSH connection successful to $(hostname)"' || echo "Failed to connect to $host"
done

# ===== Final Output =====
echo "=========================================="
echo "Passwordless SSH setup completed for all workers!"
echo "You can now run HiBench without SSH password prompts."
echo "=========================================="
