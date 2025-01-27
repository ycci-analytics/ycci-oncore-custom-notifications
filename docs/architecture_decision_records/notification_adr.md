---
status: Implemented
date: 2025-01-27
deciders: Nick Van Kuren
---

# AD: Design custom notification framework with script per business need

## Context and Problem Statement

The YCCI analytics team has received requests for OnCore (Yale's Clinical Trial Management System) custom notifications and also views this feature as an on going need for end users. OnCore custom notifications will allow the team to support custom logic for sending email notifications to OnCore users in addition to the out of the box notification options provided in the application. 


## Decision Drivers

* Need to be able to deploy each custom notificaation to a Windows based server and schedule these via Windows scheduler. 
* This server is managed by another team so want to avoid needing to install extra dependencies.

## Design Conisderations and Decision

### language
Python has been selected as the language for implementing the OnCore custom notification framework. The primary reason for this is developer experience, but also for ease of deployment to a Windows server given the availability of PyInstaller which allows for bundling Python scripts as .exe files.

### Script implementation
Based on the requirements to implement a custom notification framework that can be easily deployed and scheduled, and allow for easy addition of new notification logic, the following options were considered in terms of the architecture approach:
 
* A single universal script with parameter input to create a unique instance of a custom notification. For example a custom script could take parameter inputs to set the SQL query, the body of the email and other unique attributes for the notification logic. 
* Individual script per notification. While there may be some redundancy in code (which could be refactored into a separate functions file eventually) storing the custom notifications as individual scripts makes for a simple organization structure to start up the framework. 

### Decision
Because we want to be able to easily package the scripts for deployment to the Windows server creating a script per notification allows us to easily use Pyinstaller to bundle each notification as a .exe file that can then be scheduled on the server. This removes the need to install Python and library dependencies on the server and avoids needing to create batch scripts with each set of unique variables/input if we were to go with a single universal script approach. For this reason, and because we do not know the long term scope of custom notifications (will we have a few or many) it made sense to take the approach of developing a script per notification for now.
