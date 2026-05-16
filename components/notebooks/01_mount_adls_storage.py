# Databricks notebook source
# Notebboo1 : Connect databricks to azure ADLS Gen 2 storage
# When run: mounts persist until cluster is restarted

storage_account_name = "sainsuranceps01"
storage_account_key = "YOUR_STORAGE_KEY_HERE"


def mount_container(container, mount_point):

    try:
        dbutils.fs.mount(
            source=f"wsab://{container}@{storage_account_name}.blob.core.windows.net",
            mount_point=mount_point,
            extra_configs={
                f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key
            },
        )
        print(f"Mounted {container} at {mount_point}")

    except Exception as e:
        print(f"Already mounted or error for {container}: {str(e)[:80]}")


mount_container("raw", "/mnt/raw")
mount_container("transformed", "/mnt/transformed")
mount_container("curated", "/mnt/curated")

# COMMAND ----------
