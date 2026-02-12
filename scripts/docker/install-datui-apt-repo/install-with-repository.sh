#!/bin/sh
set -e
echo "Installing Datui APT repository..."
curl -fsSL https://derekwisong.github.io/datui-apt/public.key | gpg --dearmor | tee /usr/share/keyrings/datui-archive-keyring.gpg > /dev/null
echo "deb [signed-by=/usr/share/keyrings/datui-archive-keyring.gpg] https://derekwisong.github.io/datui-apt/ ./" | tee /etc/apt/sources.list.d/datui.list > /dev/null

echo "Updating APT repository..."
apt-get update

echo "Installing Datui..."
apt-get install -y datui

echo "Verifying Datui installation..."
datui --version
which datui

echo "Verifying Datui manpage..."
man -w datui

echo "Datui installation complete!"