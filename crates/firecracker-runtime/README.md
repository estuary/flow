## Requirements
* CNI plugins in `/opt/cni/bin`
    * From [Container Networking](`https://github.com/containernetworking/plugins`) using `build_linux.sh`
        * `ptp`
        * `host-local`
        * `firewall`
    * From `https://github.com/awslabs/tc-redirect-tap`
        * `tc-redirect-tap`
* `cnitool` binary on path
    * Found in https://github.com/containernetworking/cni/tree/main/cnitool
        ```
        go get github.com/containernetworking/cni
        go install github.com/containernetworking/cni/cnitool
        ```
* `firecracker` and `jailer` binaries on path
    * From releases page on https://github.com/firecracker-microvm/firecracker
* `virt-make-fs` on path

  **Note**: This will go away when we switch to using `containerd`
    * From `libguestfs`. On Ubuntu: `sudo apt-get install libguestfs-tools`

* Must be run as root:
    * Creating filesystem images using `mount` needs root.
    * All of the various things CNI networking does needs root, or at least `CAP_NET_ADMIN`
    * `jailer` needs all sorts of permissions involving cgroups, network namespaces, mounts etc
    * `firecracker` needs to call `/dev/kvm`



## Resources
* https://github.com/firecracker-microvm/firecracker-go-sdk