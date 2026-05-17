import os
import json
import time
import requests
import firebase_admin
from firebase_admin import credentials, firestore, db

# ==========================================
# CẤU HÌNH CỨNG
# ==========================================
ADMIN_WALLET = "0:54efc445116ebc8fd644b5a2e88728ebff91aebf14d2245de1ec76190c60997e" 
ADMIN_TELEGRAM_ID = "-1003848712775"
ADMIN_TOPIC_ID = 134
PRICE_SPREAD = 0.03

# ==========================================
# CẤU HÌNH NHẠY CẢM
# ==========================================
BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
FIREBASE_SERVICE_ACCOUNT = os.environ.get("FIREBASE_SERVICE_ACCOUNT")

if not all([BOT_TOKEN, DATABASE_URL, FIREBASE_SERVICE_ACCOUNT]):
    print("❌ LỖI: Thiếu biến môi trường nhạy cảm.")
    exit()

try:
    cred_dict = json.loads(FIREBASE_SERVICE_ACCOUNT)
    cred = credentials.Certificate(cred_dict)
    firebase_admin.initialize_app(cred, {'databaseURL': DATABASE_URL})
    db_fs = firestore.client()
    db_rt = db
    print("✅ Kết nối Firebase thành công!")
except Exception as e:
    print(f"❌ Lỗi kết nối Firebase: {e}")
    exit()

def send_telegram_msg(chat_id, text, thread_id=None):
    try:
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        if thread_id:
            payload["message_thread_id"] = thread_id
        requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json=payload, timeout=5)
    except Exception as e:
        print(f"Lỗi gửi Tele: {e}")

def round10(num):
    return float(f"{num:.10f}")

# ==========================================
# CHƯƠNG TRÌNH CHÍNH (WORKER)
# ==========================================
def main():
    print("⏳ Đang khởi động Bot Quét TON...")
    
    # 🛡️ LỚP KHIÊN 1: LƯU TRỮ ID GIAO DỊCH VÀO RAM
    processed_txs = set() 
    pending_orders_ram = []
    
    try:
        print("🔄 Đang nạp lịch sử giao dịch vào RAM để tiết kiệm Read...")
        time_limit = int(time.time() * 1000) - (2 * 3600 * 1000) # 2 tiếng trước
        recent_txs = db_fs.collection('transactions').where('createdAt', '>=', time_limit).stream()
        for doc in recent_txs:
            processed_txs.add(doc.id)
        print(f"✅ Đã nạp {len(processed_txs)} giao dịch cũ vào RAM bảo vệ.")
    except Exception as e:
        print(f"⚠️ Lỗi nạp cache: {e}")

    print("🚀 BOT BẮT ĐẦU HOẠT ĐỘNG (Quét 5s/lần)...\n")
    
    start_time = time.time()
    MAX_RUN_TIME = 5.5 * 3600 

    while True:
        if time.time() - start_time > MAX_RUN_TIME:
            print("⏳ Tự động tắt chờ Cronjob lượt sau gọi dậy!")
            break

        try:
            time.sleep(5)
            
            # Sức chứa RAM mở rộng lên 10.000 để dư sức gánh hệ thống lớn
            if len(processed_txs) > 10000:
                processed_txs.clear()

            # Tăng limit lên 100 để không lọt lưới nếu bị spam
            res = requests.get(f"https://tonapi.io/v2/accounts/{ADMIN_WALLET}/events?limit=100", timeout=10)
            if res.status_code != 200: continue
            
            events = res.json().get('events', [])
            events.reverse() # Lật ngược: Xử lý từ đơn cũ nhất tiến dần đến đơn mới nhất

            if not events and not pending_orders_ram: 
                continue

            # 2. LẤY GIÁ TON 
            ton_price_usd = 0
            is_price_alive = False

            try:
                binance_res = requests.get("https://api.binance.com/api/v3/ticker/price?symbol=TONUSDT", timeout=5)
                if binance_res.status_code == 200:
                    ton_price_usd = float(binance_res.json()['price'])
                    is_price_alive = True
            except Exception:
                pass

            if not is_price_alive:
                try:
                    kucoin_res = requests.get("https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=TON-USDT", timeout=5)
                    if kucoin_res.status_code == 200:
                        ton_price_usd = float(kucoin_res.json()['data']['price'])
                        is_price_alive = True
                except Exception:
                    pass

            ton_deposit_rate_usd = ton_price_usd * (1 - PRICE_SPREAD)

            # 3. ƯU TIÊN XỬ LÝ ĐƠN TREO TỪ RAM
            if is_price_alive and pending_orders_ram:
                for pending in pending_orders_ram[:]: 
                    uid, ton_received, tx_hash = pending['uid'], pending['ton_received'], pending['tx_hash']
                    
                    tx_ref = db_fs.collection('transactions').document(tx_hash)
                    tx_doc = tx_ref.get()
                    if not tx_doc.exists or tx_doc.to_dict().get('status') == 'success':
                        pending_orders_ram.remove(pending)
                        continue
                    
                    wallet_ref = db_rt.reference(f"user_wallets/{uid}")
                    safe_usd_value = round(ton_received * ton_deposit_rate_usd, 6)

                    batch = db_fs.batch()
                    user_ref = db_fs.collection('users').document(uid)
                    batch.update(tx_ref, {'amountUSD': safe_usd_value, 'status': 'success'})

                    user_doc = user_ref.get()
                    current_history = user_doc.to_dict().get('transactionHistory', []) if user_doc.exists else []
                    
                    # Cập nhật lại trạng thái lịch sử thay vì nhét thêm 1 dòng nữa
                    for record in current_history:
                        if record.get('txHash') == tx_hash:
                            record['status'] = 'completed'
                            record['amount'] = safe_usd_value
                            break
                    
                    batch.set(user_ref, {'transactionHistory': current_history[:50]}, merge=True)
                    
                    # Chống Race Condition bằng Transaction khi gỡ RAM
                    def update_wallet_ram(current_data):
                        if current_data is None: return current_data
                        current_data['balance'] = round10(float(current_data.get('balance', 0)) + safe_usd_value)
                        current_data['lockedBalance'] = round10(float(current_data.get('lockedBalance', 0)) + safe_usd_value)
                        current_data['totalDepositedUSD'] = float(current_data.get('totalDepositedUSD', 0)) + safe_usd_value
                        return current_data

                    try:
                        batch.commit()
                        wallet_ref.transaction(update_wallet_ram)
                        
                        display_crypto = f"{ton_received:.4f} TON"
                        print(f"✅ [GỠ TREO RAM] +{safe_usd_value} USDT cho ID {uid}")
                        send_telegram_msg(uid, f"🎉 <b>Deposit Processed!</b>\n\nYour delayed deposit of <b>{display_crypto}</b> has been processed.\n<b>+{safe_usd_value} USDT</b> has been added!")
                        pending_orders_ram.remove(pending)
                    except Exception:
                        pass

            # 4. XỬ LÝ CÁC GIAO DỊCH MỚI TRỰC TIẾP TỪ EVENTS MÀ KHÔNG DÙNG LAST_PROCESSED_TIME
            current_time = int(time.time())
            
            for event in events:
                tx_hash = event['event_id'] 

                # 🛡️ KHIÊN 1: Chặn đơn cổ đại (vượt quá 1 tiếng trước) để bot không bới móc quá khứ
                if event['timestamp'] < (current_time - 3600):
                    continue

                # 🛡️ KHIÊN 2: Bỏ qua đơn lơ lửng Mempool (Chờ xác nhận xong mới đớp)
                if event.get('in_progress') is True:
                    continue
                
                # 🛡️ KHIÊN 3: Chặn đơn đã được lưu trong RAM (Chống cộng tiền 2 lần)
                if tx_hash in processed_txs:
                    continue
                
                actions = event.get('actions', [])
                ton_received = 0
                usdt_received = 0
                uid = ""
                display_crypto = ""

                for a in actions:
                    if a.get('status') != 'ok': continue
                    
                    if a.get('type') == 'TonTransfer':
                        ton_data = a.get('TonTransfer', {})
                        receiver = ton_data.get('recipient', {}).get('address', '')
                        memo = ton_data.get('comment', '')
                        
                        if receiver.lower() == ADMIN_WALLET.lower() and memo:
                            ton_received = int(ton_data.get('amount', 0)) / 1e9
                            uid = str(memo).strip()
                            display_crypto = f"{ton_received:.4f} TON"
                            break

                    elif a.get('type') == 'JettonTransfer':
                        jetton_data = a.get('JettonTransfer', {})
                        receiver = jetton_data.get('recipient', {}).get('address', '')
                        memo = jetton_data.get('comment', '')
                        jetton_info = jetton_data.get('jetton', {})
                        
                        if receiver.lower() == ADMIN_WALLET.lower() and memo and jetton_info.get('symbol') == 'USDT':
                            usdt_received = int(jetton_data.get('amount', 0)) / 1e6
                            uid = str(memo).strip()
                            display_crypto = f"{usdt_received:.2f} USDT"
                            break

                if not uid.isdigit() or (ton_received <= 0 and usdt_received <= 0):
                    processed_txs.add(tx_hash) 
                    continue

                tx_ref = db_fs.collection('transactions').document(tx_hash)
                user_ref = db_fs.collection('users').document(uid)
                wallet_ref = db_rt.reference(f"user_wallets/{uid}")
                wallet_snap = wallet_ref.get()

                if not wallet_snap: 
                    processed_txs.add(tx_hash)
                    continue 

                if is_price_alive or usdt_received > 0:
                    if usdt_received > 0:
                        safe_usd_value = round(usdt_received * (1 - PRICE_SPREAD), 6)
                    else:
                        safe_usd_value = round(ton_received * ton_deposit_rate_usd, 6)

                    current_deposited = float(wallet_snap.get('totalDepositedUSD', 0))

                    batch = db_fs.batch()
                    batch.create(tx_ref, {
                        'uid': uid, 'type': 'deposit', 'amountTON': ton_received, 
                        'amountUSDT_Jetton': usdt_received, 'amountUSD': safe_usd_value, 
                        'txHash': tx_hash, 'status': 'success', 'createdAt': int(time.time() * 1000)
                    })
                    
                    user_doc = user_ref.get()
                    current_history = user_doc.to_dict().get('transactionHistory', []) if user_doc.exists else []
                    
                    dep_record = {
                        'id': f"DEP_{int(time.time() * 1000)}_{uid}",
                        'type': 'deposit',
                        'amount': safe_usd_value,
                        'network': 'tele',
                        'username': uid,
                        'txHash': tx_hash,
                        'status': 'completed',
                        'created_at': int(time.time() * 1000)
                    }
                    current_history.insert(0, dep_record)
                    batch.set(user_ref, {'transactionHistory': current_history[:50], 'hasDeposited3USD': (current_deposited + safe_usd_value) >= 3}, merge=True)
                    
                    # Dùng Transaction cho nạp tiền bình thường
                    def update_wallet_new(current_data):
                        if current_data is None: return current_data
                        current_data['balance'] = round10(float(current_data.get('balance', 0)) + safe_usd_value)
                        current_data['lockedBalance'] = round10(float(current_data.get('lockedBalance', 0)) + safe_usd_value)
                        current_data['totalDepositedUSD'] = float(current_data.get('totalDepositedUSD', 0)) + safe_usd_value
                        return current_data

                    try:
                        batch.commit()
                        wallet_ref.transaction(update_wallet_new)
                        
                        processed_txs.add(tx_hash)
                        print(f"✅ [NẠP AUTO] +{safe_usd_value} USDT cho ID {uid}")

                        user_msg = f"🎉 <b>DEPOSIT SUCCESSFUL!</b>\n\nThe system has processed your deposit of <b>{display_crypto}</b>.\n\n✅ <b>+{safe_usd_value} USDT</b> added to your account."
                        send_telegram_msg(uid, user_msg)
                        
                        admin_msg = f"🔔 <b>NẠP AUTO THÀNH CÔNG!</b>\n👤 <b>ID:</b> <code>{uid}</code>\n💎 <b>Cộng:</b> +{safe_usd_value} USDT\n💰 <b>Vốn vào:</b> {display_crypto}\n🔍 <a href='https://tonviewer.com/transaction/{tx_hash}'>Check Tonviewer</a>"
                        send_telegram_msg(ADMIN_TELEGRAM_ID, admin_msg, thread_id=ADMIN_TOPIC_ID)
                    
                    except Exception as e:
                        print(f"⚠️ [TRÙNG LẶP] Đơn {tx_hash} đã bị bot khác xử lý trước. Bỏ qua an toàn.")
                        processed_txs.add(tx_hash)

                else:
                    batch = db_fs.batch()
                    batch.create(tx_ref, {'uid': uid, 'type': 'deposit', 'amountTON': ton_received, 'amountUSDT_Jetton': 0, 'amountUSD': 0, 'txHash': tx_hash, 'status': 'pending_manual', 'createdAt': int(time.time() * 1000)})
                    user_doc = user_ref.get()
                    current_history = user_doc.to_dict().get('transactionHistory', []) if user_doc.exists else []
                    
                    dep_record = {
                        'id': f"DEP_{int(time.time() * 1000)}_{uid}",
                        'type': 'deposit',
                        'amount': 0, 
                        'network': 'tele',
                        'username': uid,
                        'txHash': tx_hash,
                        'status': 'pending', 
                        'created_at': int(time.time() * 1000)
                    }
                    current_history.insert(0, dep_record)
                    batch.set(user_ref, {'transactionHistory': current_history[:50]}, merge=True)
                    
                    try:
                        batch.commit()
                        pending_orders_ram.append({'uid': uid, 'ton_received': ton_received, 'tx_hash': tx_hash})
                        processed_txs.add(tx_hash) 
                        
                        err_msg = f"⚠️ Đang kẹt 1 đơn <b>{display_crypto}</b> của <code>{uid}</code> trong RAM do lỗi mạng Binance."
                        send_telegram_msg(ADMIN_TELEGRAM_ID, err_msg, thread_id=ADMIN_TOPIC_ID)
                    except Exception:
                        processed_txs.add(tx_hash)

        except requests.exceptions.RequestException:
            pass 
        except Exception as e:
            print(f"❌ Lỗi vòng lặp: {e}")

if __name__ == "__main__":
    main()
