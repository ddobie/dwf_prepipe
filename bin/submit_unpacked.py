import inotify.adapters
import os

notifier = inotify.adapters.Inotify()
notifier.add_watch('/fred/oz100/pipes/DWF_PIPE/CTIO_PUSH/untar/')

for event in notifier.event_gen():
    if event is not None:
        if 'IN_CREATE' in event[1]:
            filename = event[3]
            print(filename)
            if filename.endswith('.jp2'):
                os.system('source .bashrc')
                os.system('python dwf_prepipe_processccd.py -i ' + str(filename))


