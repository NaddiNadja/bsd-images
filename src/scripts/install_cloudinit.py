#!/usr/bin/env python3
"""
Create disk image using a cloud image
=====================================

This will produce disk images for all the system images described in config. section:

* ``system_imaging.images``

You can reduce this by providing a case-incensitive fnmatch pattern as input to the
script via workflow, such as these::

    # This will build all images
    with:
      pattern: "*"

    # This will build all those starting with "debian"
    with:
      pattern: "debian*"

Retargetable: False
-------------------

This script only runs on the iniator; due to the use of 'shutil', 'download' etc.
"""
import errno
import logging as log
from pathlib import Path

from cijoe.core.misc import decompress_file, download
from cijoe.qemu.wrapper import Guest


def cloudimage_from_nuageimage(cijoe, image: dict):
    """
    Build a cloudimage, using qemu and a nuageimage, and copy it to the cloudimage location
    """

    if not (nuage := image.get("nuage", {})):
        log.error("missing .nuage entry in configuration file")
        return errno.EINVAL

    nuage_image_path = Path(nuage.get("path"))
    nuage_image_url = nuage.get("url")
    nuage_image_metadata_path = Path(nuage.get("metadata_path"))
    nuage_image_userdata_path = Path(nuage.get("userdata_path"))

    if not (cloud := image.get("cloud", {})):
        log.error("missing .cloud entry in configuration file")
        return errno.EINVAL

    if not nuage_image_path.exists():
        nuage_image_path.parent.mkdir(parents=True, exist_ok=True)

        err, _ = download(nuage_image_url, nuage_image_path)
        if err:
            log.error(f"download({nuage_image_url}), {nuage_image_path}: failed")
            return err

    if nuage_image_decompressed_path := nuage.get("decompressed_path", None):
        nuage_image_decompressed_path = Path(nuage_image_decompressed_path).resolve()
        if not nuage_image_decompressed_path.exists():
            nuage_image_decompressed_path.parent.mkdir(parents=True, exist_ok=True)

            err, _ = decompress_file(nuage_image_path, nuage_image_decompressed_path)
            if err:
                log.error(
                    f"decompress_file({nuage_image_path}, {nuage_image_decompressed_path}): failed"
                )
                return err

        nuage_image_path = nuage_image_decompressed_path

    if (system_label := image.get("system_label", None)) is None:
        log.error("missing .system_label entry in configuration file")
        pass

    # Get the first guest with a matching system_label
    guest_name = None
    for cur_guest_name, cur_guest in cijoe.getconf("qemu.guests", {}).items():
        guest_system_label = cur_guest.get("system_label", None)
        if guest_system_label is None:
            log.error(f"guest_name({cur_guest_name}) is missing 'system_label'")
            return errno.EINVAL

        if guest_system_label == system_label:
            guest_name = cur_guest_name
            break

    if guest_name is None:
        log.error("Could not find a guest to use for cloudimage creation")
        return errno.EINVAL

    guest = Guest(cijoe, cijoe.config, guest_name)
    guest.kill()  # Ensure the guest is *not* running
    guest.initialize(nuage_image_path)  # Initialize using the nuageimage

    # Create seed.img, with data and meta embedded
    guest_metadata_path = guest.guest_path / "meta-data"
    err, _ = cijoe.run_local(f"cp {nuage_image_metadata_path} {guest_metadata_path}")
    guest_userdata_path = guest.guest_path / "user-data"
    err, _ = cijoe.run_local(f"cp {nuage_image_userdata_path} {guest_userdata_path}")

    # This uses mkisofs instead of cloud-localds, such that it works on macOS and Linux,
    # the 'mkisofs' should be available with 'cdrtools'
    seed_img = guest.guest_path / "seed.img"
    nuage_cmd = " ".join(
        [
            "mkisofs",
            "-output",
            f"{seed_img}",
            "-volid",
            "cidata",
            "-joliet",
            "-rock",
            str(guest_userdata_path),
            str(guest_metadata_path),
        ]
    )
    err, _ = cijoe.run_local(nuage_cmd)
    if err:
        log.error(f"Failed creating {seed_img}")
        return err

    # Resize the .qcow file This still requires that the partitions are resized with
    # e.g. growpart as part of the nuage-init process
    cijoe.run_local(f"qemu-img info {guest.boot_img}")

    err, _ = cijoe.run_local(f"qemu-img resize {guest.boot_img} 12G")
    if err:
        log.error("Failed resizing .qcow image")
        return err

    cijoe.run_local(f"qemu-img info {guest.boot_img}")

    # Additional args to pass to the guest when starting it
    system_args = []

    system_args += ["-cdrom", f"{seed_img}"]

    err = install_cloudinit(cijoe, guest, system_args)
    if err:
        log.error("Could not install cloud-init")
        return err

    # Copy to cloud-location
    cloud_path = Path(cloud.get("path"))
    cloud_path.parent.mkdir(parents=True, exist_ok=True)
    err, _ = cijoe.run_local(f"cp {guest.boot_img} {cloud_path}")
    if err:
        log.error(f"Failed copying to {cloud_path}")
        return err

    # Compute sha256sum of the cloud-image
    err, _ = cijoe.run_local(f"sha256sum {cloud_path} > {cloud_path}.sha256")
    if err:
        log.error(f"Failed computing sha256 sum of cloud_path({cloud_path})")
        return err

    cijoe.run_local(f"ls -la {cloud_path}")
    cijoe.run_local(f"cat {cloud_path}.sha256")

    return 0


def install_cloudinit(cijoe, guest, system_args):
    log.info("initialization_tool is nuage-init, installing cloud-init")

    freebsd_transport = cijoe.getconf("cijoe.transport.default_freebsd")
    if not freebsd_transport:
        log.error("Default username and password for FreeBSD nuage-init not defined in config. It is probably freebsd:freebsd.")
        return 1

    err = guest.start(daemonize=False, extra_args=system_args + ["-no-reboot"])
    if err:
        log.error("Failure starting guest with nuage-init")
        return err

    if not guest.wait_for_termination(timeout=60):
        log.error("Guest was not terminated after 60 seconds")
        return 1

    err = guest.start(extra_args=system_args)
    if err:
        log.error("Failure starting guest to install cloud-init")
        return err

    if not guest.is_up(timeout=60):
        log.error("Guest is not up after 60 seconds")
        return 1

    cmds = [
        "su -m root -c 'pkg install -y $(pkg search -q cloud-init | head -n1)'",
        "su -m root -c 'sysrc cloudinit_enable=YES'",
        "su -m root -c 'poweroff'",
    ]
    for cmd in cmds:
        err, _ = cijoe.run(cmd, transport_name="default_freebsd")
        if err:
            log.error(f"Error running cmd({cmd}): {err}")
            guest.kill()
            return err

    if not guest.wait_for_termination(timeout=60):
        log.error("Guest was not terminated after 60 seconds")
        return 1


def main(args, cijoe):
    """Provision a qemu-guest using a cloud-init image"""

    entry_name = "system-imaging.bsd-image"
    image = cijoe.getconf(entry_name, {})

    err = cloudimage_from_nuageimage(cijoe, image)
    if err:
        log.error(f"failed cloudimage_from_nuageimage(); err({err})")
        return err

    return 0
