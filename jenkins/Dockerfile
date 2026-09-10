FROM jenkins/jenkins:lts

USER root

# Install rsync, Docker CLI, and docker-compose plugin
RUN apt-get update -qq && \
    apt-get install -y rsync curl ca-certificates gnupg && \
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    chmod a+r /etc/apt/keyrings/docker.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
      https://download.docker.com/linux/debian $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
      > /etc/apt/sources.list.d/docker.list && \
    apt-get update -qq && \
    apt-get install -y docker-ce-cli docker-compose-plugin && \
    rm -rf /var/lib/apt/lists/*

# Allow jenkins user to access the docker socket
# GID 988 matches the host's docker socket group (stat -c "%g" /var/run/docker.sock)
RUN groupadd -f -g 988 docker && usermod -aG docker jenkins

# Skip the setup wizard entirely on every boot.
ENV JAVA_OPTS="-Djenkins.install.runSetupWizard=false"

# Groovy init script: runs once on first boot (empty jenkins_home).
# Creates the 'admin' user from JENKINS_ADMIN_PASSWORD env var and switches
# to full-admin security realm so the instance is usable immediately without
# any manual setup steps.
# If an admin user already exists (volume reuse) the script is a no-op.
RUN mkdir -p /usr/share/jenkins/ref/init.groovy.d
COPY init.groovy /usr/share/jenkins/ref/init.groovy.d/init-admin-user.groovy

USER jenkins
