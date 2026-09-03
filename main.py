import os
from flask import Flask, render_template_string, request, redirect, url_for, session, flash
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError

app = Flask(__name__)
app.secret_key = os.urandom(24)

# Temporary storage for multi-step phone login
temp_login_data = {}

INDEX_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Official Titan & Devil - Admin Login</title>
    <style>
        body { background: #0f172a; color: #f8fafc; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }
        .login-box { background: #1e293b; padding: 30px; border-radius: 12px; box-shadow: 0 10px 25px rgba(0,0,0,0.5); width: 100%; max-width: 400px; }
        h2 { text-align: center; color: #38bdf8; margin-bottom: 20px; }
        .tab-menu { display: flex; margin-bottom: 20px; border-bottom: 2px solid #334155; }
        .tab-btn { flex: 1; background: none; border: none; color: #94a3b8; padding: 10px; font-weight: bold; cursor: pointer; transition: 0.3s; }
        .tab-btn.active { color: #38bdf8; border-bottom: 2px solid #38bdf8; }
        .tab-content { display: none; }
        .tab-content.active { display: block; }
        label { display: block; margin-bottom: 8px; font-size: 14px; color: #cbd5e1; }
        input { width: 100%; padding: 10px; margin-bottom: 15px; background: #0f172a; border: 1px solid #475569; border-radius: 6px; color: #fff; box-sizing: border-box; }
        button { width: 100%; padding: 12px; background: #0284c7; border: none; border-radius: 6px; color: white; font-weight: bold; cursor: pointer; transition: 0.2s; }
        button:hover { background: #0ea5e9; }
        .flash { background: #991b1b; color: #f8fafc; padding: 10px; border-radius: 6px; margin-bottom: 15px; font-size: 13px; text-align: center; }
    </style>
</head>
<body>
    <div class="login-box">
        <h2>🛡️ ADMIN PANEL LOGIN</h2>
        
        {% with messages = get_flashed_messages() %}
          {% if messages %}
            <div class="flash">{{ messages[0] }}</div>
          {% endif %}
        {% endwith %}

        <div class="tab-menu">
            <button class="tab-btn active" onclick="switchTab('session')">Session ID</button>
            <button class="tab-btn" onclick="switchTab('phone')">Phone / OTP</button>
        </div>

        <!-- TAB 1: Session ID Login (Recommended for Cloud / Mobile) -->
        <div id="session-tab" class="tab-content active">
            <form action="/login_session" method="POST">
                <label>API ID:</label>
                <input type="text" name="api_id" placeholder="e.g. 1234567" required>
                
                <label>API Hash:</label>
                <input type="text" name="api_hash" placeholder="e.g. abc123xyz..." required>
                
                <label>Telegram String Session ID:</label>
                <input type="text" name="string_session" placeholder="Paste your Telethon String Session here..." required>
                
                <button type="submit">Login via Session ID</button>
            </form>
        </div>

        <!-- TAB 2: Phone Number & OTP Login -->
        <div id="phone-tab" class="tab-content">
            <form action="/send_otp" method="POST">
                <label>API ID:</label>
                <input type="text" name="api_id" placeholder="e.g. 1234567" required>
                
                <label>API Hash:</label>
                <input type="text" name="api_hash" placeholder="e.g. abc123xyz..." required>
                
                <label>Phone Number (with Country Code):</label>
                <input type="text" name="phone" placeholder="+977XXXXXXXXXX" required>
                
                <button type="submit">Send OTP Code</button>
            </form>
        </div>
    </div>

    <script>
        function switchTab(tabName) {
            document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));
            
            if(tabName === 'session') {
                document.querySelectorAll('.tab-btn')[0].classList.add('active');
                document.getElementById('session-tab').classList.add('active');
            } else {
                document.querySelectorAll('.tab-btn')[1].classList.add('active');
                document.getElementById('phone-tab').classList.add('active');
            }
        }
    </script>
</body>
</html>
"""

OTP_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Verify OTP - Admin Panel</title>
    <style>
        body { background: #0f172a; color: #f8fafc; font-family: sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }
        .box { background: #1e293b; padding: 30px; border-radius: 12px; width: 100%; max-width: 400px; box-shadow: 0 10px 25px rgba(0,0,0,0.5); }
        h2 { text-align: center; color: #38bdf8; }
        input { width: 100%; padding: 10px; margin-bottom: 15px; background: #0f172a; border: 1px solid #475569; border-radius: 6px; color: #fff; box-sizing: border-box; }
        button { width: 100%; padding: 12px; background: #16a34a; border: none; border-radius: 6px; color: white; font-weight: bold; cursor: pointer; }
        button:hover { background: #15803d; }
        .flash { background: #991b1b; color: #f8fafc; padding: 10px; border-radius: 6px; margin-bottom: 15px; font-size: 13px; text-align: center; }
    </style>
</head>
<body>
    <div class="box">
        <h2>🔐 ENTER TELEGRAM OTP</h2>
        {% with messages = get_flashed_messages() %}
          {% if messages %}
            <div class="flash">{{ messages[0] }}</div>
          {% endif %}
        {% endwith %}
        <form action="/verify_otp" method="POST">
            <label>OTP Code received on Telegram:</label>
            <input type="text" name="code" placeholder="Enter code..." required>
            <button type="submit">Verify & Login</button>
        </form>
    </div>
</body>
</html>
"""

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Devil Prediction - Admin Dashboard</title>
    <style>
        body { background: #0f172a; color: #f8fafc; font-family: sans-serif; margin: 0; padding: 20px; }
        .container { max-width: 800px; margin: auto; background: #1e293b; padding: 30px; border-radius: 12px; box-shadow: 0 10px 25px rgba(0,0,0,0.5); }
        h1 { color: #38bdf8; text-align: center; }
        .logout { float: right; background: #ef4444; color: white; padding: 8px 15px; text-decoration: none; border-radius: 6px; font-weight: bold; font-size: 14px; }
        .form-group { margin-bottom: 15px; }
        label { display: block; margin-bottom: 5px; color: #cbd5e1; }
        input, select, textarea { width: 100%; padding: 10px; background: #0f172a; border: 1px solid #475569; border-radius: 6px; color: #fff; box-sizing: border-box; }
        button { background: #22c55e; color: white; padding: 12px; border: none; border-radius: 6px; width: 100%; font-weight: bold; cursor: pointer; font-size: 16px; margin-top: 10px; }
        button:hover { background: #16a34a; }
    </style>
</head>
<body>
    <div class="container">
        <a href="/logout" class="logout">Logout</a>
        <h1>DEVIL PREDICTION ADMIN PANEL</h1>
        <p style="text-align: center; color: #94a3b8;">Logged in successfully via Telegram Session/Account.</p>
        <hr style="border-color: #334155; margin-bottom: 20px;">

        <h3>Create Paid Content / Match Prediction</h3>
        <form action="/publish" method="POST">
            <div class="form-group">
                <label>Match Name:</label>
                <input type="text" name="match_name" placeholder="e.g. India vs Australia" required>
            </div>
            <div class="form-group">
                <label>Content Category:</label>
                <select name="category">
                    <option value="session">Session</option>
                    <option value="match">Match Winner</option>
                    <option value="toss">Toss</option>
                    <option value="combo">All in Combo</option>
                </select>
            </div>
            <div class="form-group">
                <label>Match Date & Time:</label>
                <input type="datetime-local" name="match_time" required>
            </div>
            <div class="form-group">
                <label>Description / Prediction Details:</label>
                <textarea name="description" rows="4" placeholder="Enter your paid prediction content here..." required></textarea>
            </div>
            <div class="form-group">
                <label>Price (₹):</label>
                <input type="number" name="price" placeholder="499" required>
            </div>
            <div class="form-group">
                <label>UPI ID for Payment:</label>
                <input type="text" name="upi_id" placeholder="yourname@paytm" required>
            </div>
            <button type="submit">Publish Locked Content</button>
        </form>
    </div>
</body>
</html>
"""

@app.route('/')
def index():
    if session.get('logged_in'):
        return render_template_string(DASHBOARD_HTML)
    return render_template_string(INDEX_HTML)

@app.route('/login_session', methods=['POST'])
def login_session():
    api_id = request.form.get('api_id')
    api_hash = request.form.get('api_hash')
    string_session = request.form.get('string_session')

    try:
        client = TelegramClient(StringSession(string_session), int(api_id), api_hash)
        client.connect()
        if client.is_user_authorized():
            session['logged_in'] = True
            flash("Logged in successfully via Session ID!")
            return redirect(url_for('index'))
        else:
            flash("Invalid Session ID or Unauthorized account!")
            return redirect(url_for('index'))
    except Exception as e:
        flash(f"Error: {str(e)}")
        return redirect(url_for('index'))

@app.route('/send_otp', methods=['POST'])
def send_otp():
    api_id = request.form.get('api_id')
    api_hash = request.form.get('api_hash')
    phone = request.form.get('phone')

    try:
        client = TelegramClient(StringSession(), int(api_id), api_hash)
        client.connect()
        sent = client.send_code_request(phone)
        
        # Save temporary data in memory/dict for next step
        temp_login_data['client'] = client
        temp_login_data['phone'] = phone
        temp_login_data['phone_code_hash'] = sent.phone_code_hash
        
        return render_template_string(OTP_HTML)
    except Exception as e:
        flash(f"Failed to send OTP: {str(e)}")
        return redirect(url_for('index'))

@app.route('/verify_otp', methods=['POST'])
def verify_otp():
    code = request.form.get('code')
    client = temp_login_data.get('client')
    phone = temp_login_data.get('phone')
    phone_code_hash = temp_login_data.get('phone_code_hash')

    if not client:
        flash("Session expired. Please try again.")
        return redirect(url_for('index'))

    try:
        client.sign_in(phone, code, phone_code_hash=phone_code_hash)
        session['logged_in'] = True
        flash("Phone login successful!")
        return redirect(url_for('index'))
    except PhoneCodeInvalidError:
        flash("Invalid OTP Code entered! Please try again.")
        return render_template_string(OTP_HTML)
    except SessionPasswordNeededError:
        flash("Two-Step Verification (Password) is enabled on this account. Please use Session ID login instead.")
        return redirect(url_for('index'))
    except Exception as e:
        flash(f"Error: {str(e)}")
        return redirect(url_for('index'))

@app.route('/publish', methods=['POST'])
def publish():
    if not session.get('logged_in'):
        return redirect(url_for('index'))
    
    # Process your match prediction data here
    match_name = request.form.get('match_name')
    flash(f"Match '{match_name}' published successfully!")
    return redirect(url_for('index'))

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
