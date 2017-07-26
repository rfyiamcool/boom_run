# coding: utf-8

import time
import logging
import random
import smtplib
from email.mime.text import MIMEText

from config import mail_conf

logging.basicConfig()


class MailHandler(object):
    def __init__(self, command='', recipients=mail_conf['recipients']):
        # 邮件服务器
        self.mail_host = mail_conf['host']
        # 收件人
        self.recipients = recipients
        self.username = mail_conf['username']
        self.passwd = mail_conf['passwd']
        # 邮箱后缀
        self.mail_postfix = mail_conf["mail_postfix"]
        # 名义上的发送者
        self.sender = mail_conf['sender']
        self.command = command

        self.logger = logging.getLogger("root")

    def send_mail(self, title, html_content):
        success_flag = False
        count = 0
        while success_flag is False and count < 3:
            count += 1
            success_flag = self._send_mail(title, html_content)
            if success_flag is False:
                time.sleep(random.randint(1, 5))

        self.logger.info("send mail, recipients:%s,  title:%s, result:%s",
                        self.recipients, title, success_flag)
        return success_flag

    def _send_mail(self, title, html_content):
        '''
        :param title: 标题
        :param html_content: 内容
        :return:
        '''
        # 发信人的信息，收到信后，将按照设置显示
        me = self.sender + "<" + self.username + "@" + self.mail_postfix + ">"
        # 创建一个实例，这里设置为html格式邮件
        msg = MIMEText(html_content, _subtype='html', _charset='utf-8')
        msg['Subject'] = title  # 设置主题
        msg['From'] = me
        msg['To'] = ";".join(self.recipients)
        try:
            s = smtplib.SMTP()
            s.connect(self.mail_host)  # 连接smtp服务器
            s.login(self.username, self.passwd)  # 登陆服务器
            s.sendmail(me, self.recipients, msg.as_string())  # 发送邮件
            s.close()
            return True
        except Exception, ex:
            self.logger.error(ex, exc_info=True)
            return False


if __name__ == "__main__":
    m = MailHandler()
    m.send_mail("aaa", "sdfsdfsf")
