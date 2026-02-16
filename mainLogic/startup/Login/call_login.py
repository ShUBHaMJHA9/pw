from mainLogic.startup.Login.sudat import Login
from mainLogic.utils.glv import Global
import json
import time

from mainLogic.utils.glv_var import debugger


class LoginInterface:

    @staticmethod
    def check_valid_10_dig_number(num):
        import re
        return bool(re.match(r'^\d{10}$', num))

    @staticmethod
    def cli(phone=None,debug=False):

        whatsapp = False
        if phone and phone[:2] == "wa":
            whatsapp = True
            phone = phone[2:]

        ph_num = phone if phone else (input("Enter your 10 digit phone number: ") if not debug else "1234567890")

        if not LoginInterface.check_valid_10_dig_number(ph_num):
            print("Please enter a valid 10 digit phone number.")
            return

        lg = Login(ph_num, debug=debug)

        if lg.gen_otp(otp_type="wa" if whatsapp else "phone"):
            if lg.login(input("Enter the OTP: ")):

                token = lg.token


                if debug:
                    token = json.dumps(token, indent=4)
                    print(token)

                from beta.update import UpdateJSONFile

                from mainLogic.utils.glv_var import PREFS_FILE
                u = UpdateJSONFile(PREFS_FILE, debug=debug)

                # convert to json(dict) if possible

                if isinstance(token, str):
                    try:
                        token = json.loads(token)
                        debugger.info(f"Debug Mode: Token: {token}, type: {type(token)}")
                    except json.JSONDecodeError:
                        print("Token is not a valid JSON string.")
                        return

                if debug:
                    print("Debug Mode: Updating token")
                    debugger.debug(f"Debug Mode: Token: {token}, type: {type(token)}")

                if isinstance(token, dict):
                    user_id = token.get("user", {}).get("id") or token.get("user", {}).get("_id")
                    if user_id:
                        # Maintain backward compatibility: update top-level user_id
                        u.update("user_id", user_id, debug=debug)

                # Also support multi-user storage under 'users' list in prefs
                try:
                    # Load existing prefs dict
                    prefs = u.data if isinstance(u.data, dict) else {}
                    users = prefs.get('users') if isinstance(prefs.get('users'), list) else []
                    # Build user entry
                    entry = {
                        'phone': ph_num,
                        'name': token.get('user', {}).get('firstName') if isinstance(token, dict) else None,
                        'access_token': token.get('access_token') if isinstance(token, dict) else None,
                        'token': token,
                    }
                    # Prefer id keys if available
                    uid = None
                    if isinstance(token, dict):
                        uid = token.get('user', {}).get('id') or token.get('user', {}).get('_id')
                    if uid:
                        entry['id'] = uid

                    # Replace existing entry with same id or phone, else append
                    replaced = False
                    for i, ex in enumerate(users):
                        if (uid and ex.get('id') == uid) or (ex.get('phone') == ph_num):
                            users[i] = {**ex, **entry}
                            replaced = True
                            break
                    if not replaced:
                        users.append(entry)
                    prefs['users'] = users
                    u.data = prefs
                    u.save()
                except Exception as _e:
                    debugger.error(f"Failed to persist multi-user prefs: {_e}")

                # Use a high update index so local token wins Syncer merge.
                u.update("user_update_index", int(time.time() * 1000), debug=debug)
                u.update('token', token, debug=debug)

                debugger.info("Token updated successfully.")
            else:
                debugger.error("Login failed.")
        else:
            debugger.error("Failed to generate OTP.")
