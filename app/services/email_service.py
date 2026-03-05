import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr
from flask import current_app
from email.utils import formatdate, make_msgid


def _smtp_settings():
    host = os.getenv('HOSTALIA_SMTP_HOST') or current_app.config.get('HOSTALIA_SMTP_HOST') or 'smtp.hostalia.com'
    port = int(os.getenv('HOSTALIA_SMTP_PORT') or current_app.config.get('HOSTALIA_SMTP_PORT') or 587)
    user = os.getenv('HOSTALIA_SMTP_USER') or current_app.config.get('HOSTALIA_SMTP_USER') or 'notificaciones@bythebot.com'
    password = os.getenv('HOSTALIA_SMTP_PASS') or current_app.config.get('HOSTALIA_SMTP_PASS')
    sender_name = os.getenv('HOSTALIA_SENDER_NAME') or current_app.config.get('HOSTALIA_SENDER_NAME') or 'ByTheBot Notificaciones'
    return host, port, user, password, sender_name


def send_notification_email(subject: str, body: str, to_addr: str, html_body: str | None = None) -> bool:
    try:
        host, port, user, password, sender_name = _smtp_settings()
        if not user or not password:
            current_app.logger.warning('Email disabled: SMTP user/password not set')
            return False
        if not to_addr:
            current_app.logger.warning('Email not sent: missing destination address')
            return False

        try:
            pwd_len = len(password) if password else 0
            pwd_mask = f"len={pwd_len}"
            if pwd_len > 2:
                pwd_mask += f", ends with '...{password[-2:]}'"
            current_app.logger.info(f"SMTP credentials check: user='{user}', password_status='{pwd_mask}'")
        except Exception:
            pass

        try:
            current_app.logger.info(
                f"Email notification attempt -> to={to_addr}, subject={subject}, host={host}, port={port}, user={user}, html={(html_body is not None)}"
            )
        except Exception:
            pass

        if html_body:
            msg = MIMEMultipart('alternative')
            part1 = MIMEText(body or '', 'plain', _charset='utf-8')
            part2 = MIMEText(html_body, 'html', _charset='utf-8')
            msg.attach(part1)
            msg.attach(part2)
        else:
            msg = MIMEText(body or '', _charset='utf-8')
        msg['Subject'] = subject
        msg['From'] = formataddr((sender_name, user))
        msg['To'] = to_addr
        try:
            msg['Date'] = formatdate(localtime=True)
            # Use sender domain for Message-ID to improve deliverability
            sender_domain = (user.split('@', 1)[1] if '@' in user else 'localhost')
            msg['Message-ID'] = make_msgid(domain=sender_domain)
        except Exception:
            pass
        use_ssl = False
        try:
            cfg_ssl = os.getenv('SMTP_USE_SSL') or current_app.config.get('SMTP_USE_SSL')
            if isinstance(cfg_ssl, str):
                use_ssl = cfg_ssl.strip().lower() in ('1', 'true', 'yes')
            elif isinstance(cfg_ssl, bool):
                use_ssl = bool(cfg_ssl)
        except Exception:
            use_ssl = False

        debug_enabled = False
        try:
            cfg_dbg = os.getenv('SMTP_DEBUG') or current_app.config.get('SMTP_DEBUG')
            if isinstance(cfg_dbg, str):
                debug_enabled = cfg_dbg.strip().lower() in ('1', 'true', 'yes')
            elif isinstance(cfg_dbg, bool):
                debug_enabled = bool(cfg_dbg)
        except Exception:
            debug_enabled = False

        if use_ssl or int(port) == 465:
            with smtplib.SMTP_SSL(host, port, timeout=15) as server:
                if debug_enabled:
                    try: server.set_debuglevel(1)
                    except Exception: pass
                server.login(user, password)
                send_result = server.sendmail(user, [to_addr], msg.as_string())
        else:
            with smtplib.SMTP(host, port, timeout=15) as server:
                if debug_enabled:
                    try: server.set_debuglevel(1)
                    except Exception: pass
                try:
                    server.ehlo()
                except Exception:
                    pass
                server.starttls()
                try:
                    server.ehlo()
                except Exception:
                    pass
                server.login(user, password)
                send_result = server.sendmail(user, [to_addr], msg.as_string())
        try:
            if send_result:
                current_app.logger.warning(f"Email send returned non-empty result for some recipients: {send_result}")
            else:
                current_app.logger.info(f"Email notification sent OK -> to={to_addr}, subject={subject}")
        except Exception:
            pass
        return True
    except Exception as e:
        try:
            current_app.logger.error(f"send_notification_email error: {e}", exc_info=True)
        except Exception:
            pass
        return False


