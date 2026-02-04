#!/bin/sh
set -e

# Configuration
REPO="derekwisong/datui"
BINARY_NAME="datui"
MANPAGE_NAME="datui.1.gz"
GITHUB_URL="https://github.com/$REPO/releases/latest/download"

# Does the user want to assume yes?
for arg in "$@"; do
  if [ "$arg" = "-y" ] || [ "$arg" = "--yes" ]; then
    ASSUME_YES=true
    break
  fi
done

# Identify System
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

# Normalize Architecture
case "$ARCH" in
    x86_64)  CANONICAL_ARCH="amd64" ;;
    aarch64|arm64) CANONICAL_ARCH="arm64" ;;
    *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

# If Arch user, prompt to install via an AUR helper instead
if [ -f /etc/arch-release ]; then
    echo "-------------------------------------------------------"
    echo " ARCH LINUX DETECTED"
    echo "-------------------------------------------------------"
    echo "You may install via the AUR instead if desired:"
    echo "  paru -S datui-bin  (or your preferred AUR helper)"
    echo ""
    
    # Prompt for confirmation
    printf "Would you like to continue with the direct binary install anyway? [y/N]: "
    read -r response
    case "$response" in
        [yY][eE][sS]|[yY]) 
            echo "Proceeding with binary installation..."
            ;;
        *)
            echo "Installation cancelled. Please use the AUR package."
            exit 0
            ;;
    esac
fi

# Determine latest version tag from GitHub
TAG=$(curl -sI https://github.com/$REPO/releases/latest | grep -i "location:" | awk -F/ '{print $NF}' | tr -d '\r\n')
# Trim the 'v' for the package filenames (e.g., 0.2.0)
VERSION="${TAG#v}"

echo "--- Installing $BINARY_NAME $VERSION for $OS ($CANONICAL_ARCH) ---"

# Detect Package Manager / Format
case "$OS" in
    linux*)
        if [ -f /etc/debian_version ]; then
            # on ubuntu/debian-based systems use the deb package
            FORMAT="deb"
            FILENAME="${BINARY_NAME}_${VERSION}-1_${CANONICAL_ARCH}.deb"
        elif [ -f /etc/redhat-release ] || [ -f /etc/fedora-release ]; then
            # on redhat/rpm-based systems use the rpm package
            FORMAT="rpm"
            FILENAME="${BINARY_NAME}-${VERSION}-1.${ARCH}.rpm"
        else
            # on other systems use the binary tarball
            FORMAT="tar.gz"
            FILENAME="${BINARY_NAME}-${VERSION}-${ARCH}.tar.gz"
        fi
        ;;
    *)
        echo "Unsupported OS: $OS"
        exit 1
        ;;
esac

# 4. Download
TMP_DIR=$(mktemp -d)
echo "Downloading $FILENAME... ($TMP_DIR)"
curl -sSL "$GITHUB_URL/$FILENAME" -o "$TMP_DIR/$FILENAME"

# 5. Install based on format
case "$FORMAT" in
    deb)
        echo "Installing via apt..."
        if [ "$ASSUME_YES" = true ]; then
            sudo apt-get update -qq && sudo apt-get install -y "$TMP_DIR/$FILENAME"
        else
            sudo apt-get update -qq && sudo apt-get install "$TMP_DIR/$FILENAME"
        fi
        ;;
    rpm)
        echo "Installing via dnf..."
        if [ "$ASSUME_YES" = true ]; then
            sudo dnf install -y "$TMP_DIR/$FILENAME"
        else
            sudo dnf install "$TMP_DIR/$FILENAME"
        fi
        ;;
    tar.gz)
        echo "Extracting binary to /usr/local/bin..."
        tar -xzf "$TMP_DIR/$FILENAME" -C "$TMP_DIR"
        sudo install -d /usr/local bin
        sudo install -m 755 "$TMP_DIR/$BINARY_NAME" "/usr/local/bin/$BINARY_NAME"
        # Install manpage
        echo "Installing manpage to /usr/local/share/man/man1..."
        sudo install -d /usr/local/share/man/man1
        sudo install -m 644 "$TMP_DIR/target/release/$MANPAGE_NAME" "/usr/local/share/man/man1/$MANPAGE_NAME"
        ;;
esac

# 6. Cleanup
rm -rf "$TMP_DIR"
echo "--- $BINARY_NAME installed successfully! ---"
$BINARY_NAME --version