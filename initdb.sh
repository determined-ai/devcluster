#!/bin/bash

# create users
det u login admin
det user create hamid
det u change-password hamid

det user create hamid-cadmin
det u change-password hamid-cadmin

det user create hamid-admin --admin
det u change-password hamid-admin

# set up roles
det rbac assign-role -u hamid-cadmin ClusterAdmin

