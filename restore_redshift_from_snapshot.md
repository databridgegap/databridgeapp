# Restore Redshift from Snapshot

1. Go into the redshift snapshot list
[https://ap-southeast-2.console.aws.amazon.com/redshift/home?region=ap-southeast-2#snapshots:id=]

2. Select the latest Snapshot

3. In the Actions menu, select Restore From Snapshot

4. Leave all settings as default, except set Cluster Parameter Group to **databridgespecial**, and VPC security group to **Redshift**

5. Press Restore in the bottom right corner (you might have to scroll down)

* More Information:

** [https://docs.aws.amazon.com/redshift/latest/mgmt/managing-snapshots-console.html]
** [https://docs.aws.amazon.com/redshift/latest/mgmt/rs-tutorial-using-snapshot-restore-resize-operations.html#rs-tutorial-restore-snapshot]


