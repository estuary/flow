FROM ubuntu:noble

# Install required packages.
RUN apt update -y \
   && apt install --no-install-recommends -y \
        ca-certificates \
        certbot \
        curl \
        git \
        openssh-client \
        python3-certbot-dns-google \
        python3-poetry \
        python3-venv \
   && rm -rf /var/lib/apt/lists/*

# Install the `pulumi` CLI.
RUN curl -fsSL https://get.pulumi.com/ | bash -s
RUN ln -s /root/.pulumi/bin/pulumi /usr/local/bin/pulumi

# Install the `sops` CLI.
RUN curl -L -o /usr/local/bin/sops \
      https://github.com/getsops/sops/releases/download/v3.9.1/sops-v3.9.1.linux.amd64 \
   && chmod +x /usr/local/bin/sops

# Copy in our local assets.
COPY data-plane-controller /usr/local/bin/
COPY entrypoint.sh /usr/local/bin/

# AWS profile to expect in ~/.aws/credentials
ENV AWS_PROFILE=data-plane-ops
# GCP Service Account JSON credentials path.
ENV GOOGLE_APPLICATION_CREDENTIALS=/etc/data_plane_controller.json
# Disable host-key checks when cloning our git repo.
ENV GIT_SSH_COMMAND="ssh -o StrictHostKeyChecking=no"

ENV RUST_LOG=info

CMD ["/usr/local/bin/entrypoint.sh"]

# Example of running this container locally:
# docker run --rm --net=host -it \
#   -e CONTROL_PLANE_DB_CA_CERT="$(</home/johnny/Downloads/prod-ca-2021.crt)" \
#   -e DPC_DATABASE_URL="${DATABASE_URL}" \
#   -e DPC_GITHUB_SSH_KEY="$(</home/johnny/data_plane_controller.key)" \
#   -e DPC_IAM_CREDENTIALS="$(</home/johnny/.aws/credentials)" \
#   -e DPC_SERVICE_ACCOUNT="$(</etc/data_plane_controller.json)" \
#   -e VULTR_API_KEY="${VULTR_API_KEY}" \
#   -e PGPASSWORD=${PGPASSWORD} \
#   foobar:latest