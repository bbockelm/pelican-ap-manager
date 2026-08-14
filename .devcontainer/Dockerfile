FROM almalinux:9

# Install HTCondor repository
RUN dnf install -y 'dnf-command(config-manager)' && \
    dnf config-manager --set-enabled crb && \
    dnf install -y epel-release && \
    cd /etc/yum.repos.d && \
    curl https://htcss-downloads.chtc.wisc.edu/repo/25.x/htcondor-release-current.el9.noarch.rpm > htcondor-release-current.el9.noarch.rpm && \
    dnf install -y htcondor-release-current.el9.noarch.rpm && \
    dnf clean all

# Install development tools, HTCondor, and dependencies
RUN dnf -y update && \
    dnf -y install --allowerasing \
    # Build tools
    gcc \
    gcc-c++ \
    make \
    autoconf \
    automake \
    libtool \
    git \
    tar \
    curl \
    wget \
    gettext \
    pkgconfig \
    # Go
    golang \
    # HTCondor
    condor \
    # Utilities
    procps-ng \
    vim \
    less \
    which \
    sudo \
    openssh-clients \
    # Pre-commit and linting tools
    python3-pip \
    && dnf clean all

# Install Node.js 20 from NodeSource for any JS-based linting
RUN curl -fsSL https://rpm.nodesource.com/setup_20.x | bash - && \
    dnf -y install nodejs && \
    dnf clean all

# Set up Go environment
ENV GOPATH=/go
ENV PATH=$PATH:$GOPATH/bin:/usr/local/go/bin

# Create vscode user and Go directories
RUN useradd -m -s /bin/bash -u 1000 vscode && \
    mkdir -p /home/vscode/.cache /go && \
    chown -R vscode:vscode /home/vscode /go

# Add vscode to sudoers for testing root scenarios
RUN echo "vscode ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Create condor user for HTCondor (if not already created by package)
RUN id -u condor &>/dev/null || useradd -r -s /sbin/nologin condor

# Install Go tools for development (as vscode user)
USER vscode
RUN go install golang.org/x/tools/gopls@latest && \
    go install github.com/securego/gosec/v2/cmd/gosec@latest && \
    go install honnef.co/go/tools/cmd/staticcheck@latest && \
    go install gotest.tools/gotestsum@latest && \
    go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest && \
    go install github.com/go-delve/delve/cmd/dlv@latest

# Switch back to root for remaining setup
USER root

# Install pre-commit
RUN python3 -m pip install --quiet pre-commit

# Create workspace directory
WORKDIR /workspace

# Default user for development
USER vscode

# Keep container running
CMD ["/bin/bash"]
