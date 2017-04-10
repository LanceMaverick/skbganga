import json
import requests
from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException, IPostProcessor
from Ganga.GPIDev.Schema import Schema, SimpleItem, Version
from Ganga.Utility.logging import getLogger
import Ganga.Utility.Config
config = Ganga.Utility.Config.getConfig('Configuration')

logger = getLogger()

class SkbNotifier(IPostProcessor):
    """Ganga notifier for skybeard telegram API"""
    _schema = Schema(Version(1, 0), {
        'verbose': SimpleItem(
            defvalue = False, 
            doc = 'Send telegram message on subjob completion'),
        'address': SimpleItem(
            defvalue = '', 
            doc = 'Skb server address or IP',
            optional = False),
        'key': SimpleItem(
            defvalue = ''
            doc = 'Auth key for skb server',
            optional = False),
        'chat_id': SimpleItem(
            defvalue = '',
            doc = 'ID of chat to send notifications to',
            optional = False)
        })

    _category = 'postprocessor'
    _name = 'SkbNotifier'
    order = 3

    def execute(self, job, newstatus):
	if len(job.subjobs) or \
                (newstatus == 'failed' and job.do_auto_resubmit is False) or \
                (newstatus != 'failed' and self.verbose is True):
            return self.notify(job, newstatus)
        return True

    def notify(self, job, newstatus):
        address = self.address
        key = self.key
        chat_id = self.chat_id

        msg = 'Ganga update:\n{}\n{}'.format(job.fqid, newstatus)
        url = '{}/relay{}/sendMessage'.format(address, key)
        payload = {
                'text': msg,
                'chat_id': chat_id,
                }
        try:
            request = requests.post(url, json.dumps(payload))
        except RequestException as e:
            raise PostProcessException(str(e))
        else:
            if request.status_code != 200:
                raise PostProcessException(str(e))
        return True

        

