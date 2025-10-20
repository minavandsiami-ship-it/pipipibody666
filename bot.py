import logging
import os
import random
import sqlite3
import threading
import requests
import asyncio
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Используем переменные окружения для Heroku
BOT_TOKEN = os.environ.get('BOT_TOKEN', '8141267399:AAHAxiUAhRkH3OdbkFh1LLzhYhkSoMzF0eU')
ADMIN_ID = int(os.environ.get('ADMIN_ID', '8358340380'))
CRYPTO_TOKEN = os.environ.get('CRYPTO_TOKEN', '438215:AAq9wq4oxOdokPWnWkVD9CxIKOG5eMXztcl')
BASE_DIR = os.path.dirname(__file__)
DB_FILE = os.path.join(BASE_DIR, 'shop.db')
LOCK = threading.Lock()
CRYPTO_API_URL = 'https://pay.crypt.bot/api'

STATES = [
    'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware',
    'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky',
    'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi',
    'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico',
    'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania',
    'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont',
    'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming'
]

texts = {
    'ru': {
        'welcome': '🇺🇸 Добро пожаловать в магазин 🇺🇸 FULLZMummy 🇺🇸',
        'full_info': 'Full Info:\n- Young Fullz: По годам (2004-2006), цена 3$\n- Full Info with CS: CS&700+ (4$), CS0-650 (2.5$)\n- Штаты: Мужской/Женский, по 2$ каждый\n- DL photo f+b + FULL INFO: 15$ за штуку\n- TAX RETURNS W2: 40$ за штуку',
        'full_info_btn': 'ПОЛНАЯ ИНФОРМАЦИЯ',
        'products': 'Продукты',
        'young_fullz': 'Young Fullz',
        'cs_category': 'ПОЛНАЯ ИНФОРМАЦИЯ С CS',
        'states_category': 'Штаты',
        'dl_photo': 'DL photo f+b + FULL INFO',
        'tax_returns': 'TAX RETURNS W2',
        'balance': 'Баланс',
        'topup': 'Пополнить',
        'profile': 'Личный кабинет',
        'profile_info': 'Ваш ID: {user_id}\nВаш баланс: {balance} USD\n\nИстория покупок:\n{purchases}',
        'no_purchases': 'Покупок пока нет.',
        'purchase_log': 'Куплено: {name} за {price}$ USD, количество: {quantity}, время: {timestamp}',
        'choose_crypto': 'Выберите криптовалюту для пополнения:',
        'topup_prompt': 'Введите сумму для пополнения в USD:',
        'topup_invoice': 'Оплатите {amount} {crypto} (~{usd_amount} USD) по следующей ссылке:\n{invoice_url}',
        'topup_success': 'Баланс успешно пополнен на {amount} USD! Новый баланс: {balance}',
        'topup_failed': 'Ошибка при создании инвойса: {error}',
        'admin_panel': 'Админ-панель',
        'add_product': 'Добавить продукт',
        'manual_topup': 'Пополнить баланс пользователя',
        'view_keys': 'Просмотреть ключи',
        'view_topups': 'Просмотреть пополнения',
        'choose_lang': 'Выберите язык:',
        'russian': 'Русский',
        'english': 'English',
        'change_lang': 'Сменить язык',
        'category': 'Выберите категорию:',
        'year': 'Выберите год:',
        'cs_type': 'Выберите тип CS:',
        'state': 'Выберите штат:',
        'gender': 'Выберите пол для {state}:',
        'dl_quantity': 'Введите количество для покупки (DL photo f+b + FULL INFO, 15$ за штуку):',
        'dl_confirm': 'Подтвердите покупку {quantity} шт. DL photo f+b + FULL INFO за {total}$ USD. Остаток на балансе: {balance}$. Нажмите для подтверждения:',
        'dl_confirm_btn': 'Подтвердить',
        'tax_description': 'Формат:\nФорма W-2 — это отчет о заработной плате и налогах, который включает налогооблагаемую зарплату и удержанные налоги. Содержит информацию о работодателе с EIN и о сотруднике с SSN.\nЦена: 40$ за штуку.',
        'bought': 'Куплено: {name} за {price}$ USD. Остаток: {balance}',
        'no_funds': 'Недостаточно средств.',
        'out_of_stock': '{name} закончился (остаток 0).',
        'product_not_found': 'Продукт не найден.',
        'user_not_found': 'Пользователь не найден. Попробуйте /start.',
        'product_added': 'Продукт добавлен!',
        'format_error': 'Ошибка формата. Используйте формат: name|price|stock|file_path|category|subcategory',
        'balance_topup': 'Баланс {tgt_user_id} пополнен на {amount} USD.',
        'balance_updated': 'Админ пополнил ваш баланс на {amount} USD.',
        'bonus_added': 'Вам начислен бонус 5$ на баланс!',
        'topup_log': 'Для {user_id} ({first_name} {last_name} @{username}):\nСумма: {amount}$ (в {crypto})\nВремя: {timestamp}\n\n',
        'keys_info': 'Для {user_id}:\nBTC: {addr_btc}\nLTC: {addr_ltc}\nUSDT: {addr_usdt}\n\n',
        'select_user': 'Выберите пользователя для пополнения:',
        'no_users': 'Другие пользователи не найдены.',
        'add_product_prompt': 'Отправьте данные продукта: name|price|stock|file_path|category|subcategory',
        'manual_topup_prompt': 'Введите сумму для пополнения в USD:',
        'male': 'Мужской',
        'female': 'Женский',
        'back': 'Назад',
        'back_to_menu': 'Вернуться в меню',
        'invalid_quantity': 'Введите положительное целое число.',
        'random_state': 'RANDOM STATE',
        'invalid_callback': 'Действие устарело. Пожалуйста, начните заново.',
        'file_error': 'Ошибка при доступе к файлам продукта.'
    },
    'en': {
        'welcome': '🇺🇸 Welcome to the store 🇺🇸 FULLZMummy 🇺🇸',
        'full_info': 'Full Info:\n- Young Fullz: By years (2004-2006), price 3$\n- Full Info with CS: CS&700+ (4$), CS0-650 (2.5$)\n- States: Male/Female, 2$ each\n- DL photo f+b + FULL INFO: 15$ per one\n- TAX RETURNS W2: 40$ per one',
        'full_info_btn': 'FULL INFO',
        'products': 'Products',
        'young_fullz': 'Young Fullz',
        'cs_category': 'FULL INFO WITH CS',
        'states_category': 'States',
        'dl_photo': 'DL photo f+b + FULL INFO',
        'tax_returns': 'TAX RETURNS W2',
        'balance': 'Balance',
        'topup': 'Top up',
        'profile': 'Profile',
        'profile_info': 'Your ID: {user_id}\nYour balance: {balance} USD\n\nPurchase history:\n{purchases}',
        'no_purchases': 'No purchases yet.',
        'purchase_log': 'Purchased: {name} for {price}$ USD, quantity: {quantity}, time: {timestamp}',
        'choose_crypto': 'Choose cryptocurrency for top-up:',
        'topup_prompt': 'Enter the amount to top up in USD:',
        'topup_invoice': 'Pay {amount} {crypto} (~{usd_amount} USD) using the following link:\n{invoice_url}',
        'topup_success': 'Balance successfully topped up by {amount} USD! New balance: {balance}',
        'topup_failed': 'Error creating invoice: {error}',
        'admin_panel': 'Admin panel',
        'add_product': 'Add product',
        'manual_topup': 'Top up user balance',
        'view_keys': 'View keys',
        'view_topups': 'View top-ups',
        'choose_lang': 'Choose language:',
        'russian': 'Russian',
        'english': 'English',
        'change_lang': 'Change language',
        'category': 'Choose category:',
        'year': 'Choose year:',
        'cs_type': 'Choose CS type:',
        'state': 'Choose state:',
        'gender': 'Choose gender for {state}:',
        'dl_quantity': 'Enter quantity to purchase (DL photo f+b + FULL INFO, 15$ per one):',
        'dl_confirm': 'Confirm purchase of {quantity} DL photo f+b + FULL INFO for {total}$ USD. Current balance: {balance}$. Press to confirm:',
        'dl_confirm_btn': 'Confirm',
        'tax_description': 'Format:\nForm W-2 is a wage and tax statement that reports holders taxable wages and the taxes withheld from his wages. Includes information about the employer with the EIN and information about the employee with the SSN.\nPrice: 40$ per one.',
        'bought': 'Bought: {name} for {price}$ USD. Remainder: {balance}',
        'no_funds': 'Insufficient funds.',
        'out_of_stock': '{name} out of stock (stock 0).',
        'product_not_found': 'Product not found.',
        'user_not_found': 'User not found. Try /start.',
        'product_added': 'Product added!',
        'format_error': 'Format error. Use format: name|price|stock|file_path|category|subcategory',
        'balance_topup': 'Balance of {tgt_user_id} topped up by {amount} USD.',
        'balance_updated': 'Admin topped up your balance by {amount} USD.',
        'bonus_added': 'You have been credited a 5$ bonus to your balance!',
        'topup_log': 'For {user_id} ({first_name} {last_name} @{username}):\nAmount: {amount}$ (in {crypto})\nTime: {timestamp}\n\n',
        'keys_info': 'For {user_id}:\nBTC: {addr_btc}\nLTC: {addr_ltc}\nUSDT: {addr_usdt}\n\n',
        'select_user': 'Select user to top up:',
        'no_users': 'No other users found.',
        'add_product_prompt': 'Send product data: name|price|stock|file_path|category|subcategory',
        'manual_topup_prompt': 'Enter amount to top up in USD:',
        'male': 'Male',
        'female': 'Female',
        'back': 'Back',
        'back_to_menu': 'Back to menu',
        'invalid_quantity': 'Enter a positive integer.',
        'random_state': 'RANDOM STATE',
        'invalid_callback': 'Action is outdated. Please start over.',
        'file_error': 'Error accessing product files.'
    }
}

def init_db_and_folders():
    try:
        # Создаем папки для файлов
        os.makedirs(os.path.join(BASE_DIR, '2004'), exist_ok=True)
        os.makedirs(os.path.join(BASE_DIR, '2005'), exist_ok=True)
        os.makedirs(os.path.join(BASE_DIR, '2006'), exist_ok=True)
        os.makedirs(os.path.join(BASE_DIR, 'CS700'), exist_ok=True)
        os.makedirs(os.path.join(BASE_DIR, 'CS650'), exist_ok=True)
        os.makedirs(os.path.join(BASE_DIR, 'DLphoto'), exist_ok=True)
        os.makedirs(os.path.join(BASE_DIR, 'TaxReturns'), exist_ok=True)
        for state in STATES:
            os.makedirs(os.path.join(BASE_DIR, state, 'Male'), exist_ok=True)
            os.makedirs(os.path.join(BASE_DIR, state, 'Female'), exist_ok=True)
        
        with LOCK:
            conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15)
            try:
                c = conn.cursor()
                c.execute('PRAGMA journal_mode=WAL')
                c.execute('''CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY, 
                    first_name TEXT, 
                    last_name TEXT, 
                    username TEXT, 
                    language TEXT, 
                    balance REAL DEFAULT 0.0)''')
                c.execute('''CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    name TEXT, 
                    price REAL, 
                    stock INTEGER, 
                    file_path TEXT, 
                    category TEXT, 
                    subcategory TEXT)''')
                c.execute('''CREATE TABLE IF NOT EXISTS topups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    user_id INTEGER, 
                    amount REAL, 
                    timestamp TEXT, 
                    invoice_id TEXT, 
                    status TEXT DEFAULT 'pending',
                    crypto TEXT)''')
                c.execute('''CREATE TABLE IF NOT EXISTS purchases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    user_id INTEGER, 
                    product_name TEXT, 
                    price REAL, 
                    quantity INTEGER, 
                    timestamp TEXT)''')
                conn.commit()
            finally:
                conn.close()
    except Exception as e:
        logging.error(f"Error initializing database: {e}")
        raise

async def check_crypto_token():
    for attempt in range(3):
        try:
            headers = {'Crypto-Pay-API-Token': CRYPTO_TOKEN}
            response = requests.get(f'{CRYPTO_API_URL}/getMe', headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data['ok']:
                logging.info(f"@CryptoBot token valid: {data['result']['name']}")
                return True
            else:
                logging.error(f"Invalid @CryptoBot token: {data.get('error')}")
                return False
        except Exception as e:
            logging.error(f"Error checking @CryptoBot token (attempt {attempt + 1}): {e}")
            if attempt < 2:
                await asyncio.sleep(4 * (2 ** attempt))
            else:
                raise

async def get_currency_info(crypto):
    try:
        headers = {'Crypto-Pay-API-Token': CRYPTO_TOKEN}
        response = requests.get(f'{CRYPTO_API_URL}/getCurrencies', headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data['ok']:
            for currency in data['result']:
                if currency['code'] == crypto:
                    return {
                        'min_amount': float(currency.get('min_amount', 0)),
                        'decimals': int(currency.get('decimals', 8))
                    }
            logging.error(f"Currency {crypto} not found in getCurrencies")
            return None
        logging.error(f"Error fetching currencies: {data.get('error')}")
        return None
    except Exception as e:
        logging.error(f"Error fetching currency info for {crypto}: {e}")
        return None

async def get_exchange_rate(crypto):
    try:
        headers = {'Crypto-Pay-API-Token': CRYPTO_TOKEN}
        response = requests.get(f'{CRYPTO_API_URL}/getExchangeRates', headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data['ok']:
            for rate in data['result']:
                if rate['source'] == crypto and rate['target'] == 'USD':
                    logging.info(f"Rate {crypto}/USD: {rate['rate']}")
                    return float(rate['rate'])
            logging.error(f"Rate for {crypto}/USD not found")
            return None
        logging.error(f"Error fetching exchange rates: {data.get('error')}")
        return None
    except Exception as e:
        logging.error(f"Error fetching rate for {crypto}: {e}")
        return None

async def create_crypto_invoice(user_id, usd_amount, crypto, context: ContextTypes.DEFAULT_TYPE):
    for attempt in range(3):
        try:
            currency_info = await get_currency_info(crypto)
            if not currency_info:
                logging.error(f"Currency {crypto} not supported by API")
                return None, None, f"Currency {crypto} not supported"

            exchange_rate = await get_exchange_rate(crypto)
            if not exchange_rate:
                logging.error(f"Unable to fetch rate for {crypto}")
                return None, None, f"Unable to fetch exchange rate for {crypto}"

            crypto_amount = usd_amount / exchange_rate
            min_amount = currency_info['min_amount']
            decimals = currency_info['decimals']
            if crypto_amount < min_amount:
                logging.error(f"Amount {crypto_amount} {crypto} is below minimum {min_amount}")
                return None, None, f"Amount {crypto_amount:.{decimals}f} {crypto} is below minimum {min_amount} {crypto}"

            crypto_amount_str = f"{crypto_amount:.{decimals}f}"
            headers = {'Crypto-Pay-API-Token': CRYPTO_TOKEN}
            payload = {
                'amount': crypto_amount_str,
                'asset': crypto,
                'description': f'Top-up of {usd_amount} USD in {crypto}'[:255]
            }
            logging.info(f"Creating invoice with payload: {payload}")
            await asyncio.sleep(1)
            response = requests.post(f'{CRYPTO_API_URL}/createInvoice', headers=headers, json=payload, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data['ok']:
                invoice = data['result']
                logging.info(f"Invoice created: invoice_id={invoice['invoice_id']}, pay_url={invoice['pay_url']}, crypto_amount={crypto_amount_str}, usd_amount={usd_amount}")
                return invoice['pay_url'], invoice['invoice_id'], None
            else:
                error_msg = data.get('error', 'Unknown API error')
                logging.error(f"Error creating invoice: {error_msg}, full response: {data}")
                return None, None, f"API error: {error_msg}"
        except Exception as e:
            logging.error(f"Error creating invoice for user_id {user_id} (attempt {attempt + 1}): {e}")
            if attempt < 2:
                await asyncio.sleep(4 * (2 ** attempt))
            else:
                return None, None, f"Error: {str(e)}"

async def init_products(context: ContextTypes.DEFAULT_TYPE):
    try:
        with LOCK:
            conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15)
            try:
                c = conn.cursor()
                products = [
                    ('2004', 3.0, 0, os.path.join(BASE_DIR, '2004'), 'Young Fullz', '2004'),
                    ('2005', 3.0, 0, os.path.join(BASE_DIR, '2005'), 'Young Fullz', '2005'),
                    ('2006', 3.0, 0, os.path.join(BASE_DIR, '2006'), 'Young Fullz', '2006'),
                    ('CS&700+ random state', 4.0, 0, os.path.join(BASE_DIR, 'CS700'), 'CS', '700+'),
                    ('CS0-650 RANDOM STATE', 2.5, 0, os.path.join(BASE_DIR, 'CS650'), 'CS', '0-650'),
                    ('DL photo f+b + FULL INFO', 15.0, 0, os.path.join(BASE_DIR, 'DLphoto'), 'DL photo', 'DL_random'),
                    ('TAX RETURNS W2', 40.0, 0, os.path.join(BASE_DIR, 'TaxReturns'), 'Tax Returns', 'W2')
                ]
                for state in STATES:
                    products += [
                        (f'{state} Male', 2.0, 0, os.path.join(BASE_DIR, state, 'Male'), 'States', f'{state}_Male'),
                        (f'{state} Female', 2.0, 0, os.path.join(BASE_DIR, state, 'Female'), 'States', f'{state}_Female')
                    ]
                
                for name, price, stock, file_path, category, subcategory in products:
                    c.execute('SELECT price, file_path FROM products WHERE subcategory = ?', (subcategory,))
                    row = c.fetchone()
                    if row:
                        current_price, current_path = row
                        if current_price != price or current_path != file_path:
                            c.execute('UPDATE products SET price = ?, file_path = ? WHERE subcategory = ?', (price, file_path, subcategory))
                            logging.info(f"Updated product {name}: price={price}, path={file_path}")
                    else:
                        c.execute('INSERT INTO products (name, price, stock, file_path, category, subcategory) VALUES (?, ?, ?, ?, ?, ?)',
                                (name, price, stock, file_path, category, subcategory))
                        logging.info(f"Added product {name}: price={price}, path={file_path}")
                conn.commit()
                
                c.execute('SELECT id, file_path, subcategory FROM products')
                for pid, path, subcategory in c.fetchall():
                    if os.path.isdir(path):
                        txt_files = [f for f in os.listdir(path) if f.endswith('.txt')]
                        c.execute('UPDATE products SET stock = ? WHERE id = ?', (len(txt_files), pid))
                        logging.info(f"Updated stock for {subcategory}: {len(txt_files)}")
                    else:
                        logging.warning(f"Directory {path} does not exist for subcategory {subcategory}")
                        c.execute('UPDATE products SET stock = 0 WHERE id = ?', (pid,))
                conn.commit()
            finally:
                conn.close()
        logging.info("Products initialized")
    except Exception as e:
        logging.error(f"Error initializing products: {e}")

def get_user_language(user_id):
    try:
        with LOCK:
            conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15)
            try:
                c = conn.cursor()
                c.execute('SELECT language FROM users WHERE user_id = ?', (user_id,))
                row = c.fetchone()
                return row[0] if row and row[0] else None
            finally:
                conn.close()
    except Exception as e:
        logging.error(f"Error fetching language for user_id {user_id}: {e}")
        return None

async def check_invoice_status(invoice_id):
    try:
        headers = {'Crypto-Pay-API-Token': CRYPTO_TOKEN}
        response = requests.get(f'{CRYPTO_API_URL}/getInvoices?invoice_ids={invoice_id}', headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data['ok'] and data['result']['items']:
            status = data['result']['items'][0]['status']
            logging.info(f"Invoice status {invoice_id}: {status}")
            return status
        return None
    except Exception as e:
        logging.error(f"Error checking invoice status {invoice_id}: {e}")
        return None

async def check_pending_invoices(context: ContextTypes.DEFAULT_TYPE):
    try:
        with LOCK:
            conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15)
            try:
                c = conn.cursor()
                c.execute('SELECT id, user_id, amount, invoice_id, crypto FROM topups WHERE status = ?', ('pending',))
                pending_invoices = c.fetchall()
                for topup_id, user_id, amount, invoice_id, crypto in pending_invoices:
                    status = await check_invoice_status(invoice_id)
                    if status == 'paid':
                        c.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (amount, user_id))
                        c.execute('UPDATE topups SET status = ? WHERE id = ?', ('completed', topup_id))
                        conn.commit()
                        language = get_user_language(user_id) or 'ru'
                        c.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
                        balance = c.fetchone()[0]
                        await context.bot.send_message(user_id, texts[language]['topup_success'].format(amount=amount, balance=balance))
                        logging.info(f"Invoice {invoice_id} for user_id {user_id} paid, balance topped up by {amount} USD in {crypto}")
                    elif status in ['expired', 'failed']:
                        c.execute('UPDATE topups SET status = ? WHERE id = ?', ('failed', topup_id))
                        conn.commit()
                        language = get_user_language(user_id) or 'ru'
                        await context.bot.send_message(user_id, texts[language]['topup_failed'].format(error='Invoice expired or failed'))
                        logging.info(f"Invoice {invoice_id} for user_id {user_id} expired or failed")
            finally:
                conn.close()
    except Exception as e:
        logging.error(f"Error checking pending invoices: {e}")

async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, is_admin=False):
    language = get_user_language(update.effective_user.id) or 'ru'
    context.user_data.clear()
    keyboard = [
        [InlineKeyboardButton(texts[language]['full_info_btn'], callback_data='full_info')],
        [InlineKeyboardButton(texts[language]['young_fullz'], callback_data='cat_young')],
        [InlineKeyboardButton(texts[language]['cs_category'], callback_data='cat_cs')],
        [InlineKeyboardButton(texts[language]['states_category'], callback_data='cat_states')],
        [InlineKeyboardButton(texts[language]['dl_photo'], callback_data='cat_dl')],
        [InlineKeyboardButton(texts[language]['tax_returns'], callback_data='buy_w2')],
        [InlineKeyboardButton(texts[language]['topup'], callback_data='topup')],
        [InlineKeyboardButton(texts[language]['profile'], callback_data='profile')],
        [InlineKeyboardButton(texts[language]['change_lang'], callback_data='change_lang')]
    ]
    if is_admin:
        keyboard.append([InlineKeyboardButton(texts[language]['admin_panel'], callback_data='admin')])
    reply_markup = InlineKeyboardMarkup(keyboard)
    try:
        if update.message:
            await update.message.reply_text(texts[language]['welcome'], reply_markup=reply_markup)
        else:
            await context.bot.send_message(chat_id=update.effective_user.id, text=texts[language]['welcome'], reply_markup=reply_markup)
        logging.info(f"Main menu sent to user {update.effective_user.id}")
    except Exception as e:
        logging.error(f"Error sending main menu to user {update.effective_user.id}: {e}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    first_name = update.effective_user.first_name or ''
    last_name = update.effective_user.last_name or ''
    username = update.effective_user.username or 'None'
    logging.info(f"Received /start command from user_id: {user_id}, username: @{username}")

    try:
        with LOCK:
            conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15)
            try:
                c = conn.cursor()
                c.execute('SELECT language FROM users WHERE user_id = ?', (user_id,))
                row = c.fetchone()
                logging.info(f"Checking language for user_id {user_id}, row: {row}")
                if row is None:
                    c.execute('INSERT INTO users (user_id, first_name, last_name, username, balance) VALUES (?, ?, ?, ?, ?)',
                            (user_id, first_name, last_name, username, 5.0))
                    conn.commit()
                    keyboard = [
                        [InlineKeyboardButton(texts['ru']['russian'], callback_data='lang_ru')],
                        [InlineKeyboardButton(texts['en']['english'], callback_data='lang_en')]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await update.message.reply_text(texts['ru']['choose_lang'], reply_markup=reply_markup)
                    await update.message.reply_text(texts['ru']['bonus_added'])
                    logging.info(f"Sent language selection and bonus message to user {user_id}")
                else:
                    await main_menu(update, context, user_id == ADMIN_ID)
            finally:
                conn.close()
    except Exception as e:
        logging.error(f"Error processing /start for user_id {user_id}: {e}")
        keyboard = [[InlineKeyboardButton(texts['ru']['back'], callback_data='back_main')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("Произошла ошибка. Попробуйте снова.", reply_markup=reply_markup)

async def handle_purchase(query, context, user_id, language, subcategory, quantity=1):
    logging.info(f"Handling purchase for user_id {user_id}, subcategory {subcategory}, quantity {quantity}")
    with LOCK:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15)
        try:
            c = conn.cursor()
            c.execute('SELECT id, name, price, stock, file_path FROM products WHERE subcategory = ?', (subcategory,))
            product = c.fetchone()
            if not product:
                logging.error(f"Product not found for subcategory {subcategory}")
                keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(texts[language]['product_not_found'], reply_markup=reply_markup)
                context.user_data.clear()
                return

            pid, name, price, stock, path = product
            logging.info(f"Found product: {name}, price: {price}, stock: {stock}, path: {path}")
            total_price = price * quantity

            if stock < quantity:
                logging.warning(f"Insufficient stock for {name}: requested {quantity}, available {stock}")
                keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(texts[language]['out_of_stock'].format(name=name), reply_markup=reply_markup)
                context.user_data.clear()
                return

            c.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
            balance = c.fetchone()[0]
            logging.info(f"User balance: {balance}, total price: {total_price}")

            if balance < total_price:
                logging.warning(f"Insufficient funds for user_id {user_id}: balance {balance}, required {total_price}")
                keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(texts[language]['no_funds'], reply_markup=reply_markup)
                context.user_data.clear()
                return

            if not os.path.isdir(path):
                logging.error(f"Directory {path} does not exist for {name}")
                keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(texts[language]['file_error'], reply_markup=reply_markup)
                context.user_data.clear()
                return

            txt_files = [f for f in os.listdir(path) if f.endswith('.txt')]
            if len(txt_files) < quantity:
                logging.warning(f"Not enough files in {path}: requested {quantity}, found {len(txt_files)}")
                keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(texts[language]['out_of_stock'].format(name=name), reply_markup=reply_markup)
                context.user_data.clear()
                return

            c.execute('UPDATE users SET balance = balance - ? WHERE user_id = ?', (total_price, user_id))
            selected_files = random.sample(txt_files, quantity)
            for file_to_send in selected_files:
                full_path = os.path.join(path, file_to_send)
                try:
                    with open(full_path, 'rb') as f:
                        await query.message.reply_document(f)
                    os.remove(full_path)
                    logging.info(f"File {full_path} sent and deleted")
                except Exception as e:
                    logging.error(f"Error sending file {full_path}: {e}")
                    keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await query.edit_message_text(texts[language]['file_error'], reply_markup=reply_markup)
                    context.user_data.clear()
                    return

            c.execute('UPDATE products SET stock = stock - ? WHERE id = ?', (quantity, pid))
            c.execute('INSERT INTO purchases (user_id, product_name, price, quantity, timestamp) VALUES (?, ?, ?, ?, ?)',
                    (user_id, f"{name} ({quantity} шт.)", total_price, quantity, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            conn.commit()
            keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(texts[language]['bought'].format(name=f"{name} ({quantity} шт.)", price=total_price, balance=balance - total_price), reply_markup=reply_markup)
            context.user_data.clear()
            logging.info(f"Purchase completed for user_id {user_id}: {name}, quantity {quantity}")
        finally:
            conn.close()

async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    language = get_user_language(user_id) or 'ru'
    logging.info(f"Callback from user_id: {user_id}, data: {data}")

    valid_callbacks = [
        'lang_ru', 'lang_en', 'change_lang', 'full_info', 'cat_young', 'cat_cs', 'cat_states',
        'cat_dl', 'dl_random', 'dl_confirm', 'buy_w2', 'buy_w2_confirm', 'topup', 'profile',
        'admin', 'add_product', 'manual_topup', 'view_keys', 'view_topups', 'back_main',
        'crypto_USDT', 'crypto_LTC', 'crypto_BTC'
    ] + [f'state_{state}' for state in STATES] + [f'buy_{state}_Male' for state in STATES] + \
        [f'buy_{state}_Female' for state in STATES] + ['buy_2004', 'buy_2005', 'buy_2006', 'buy_700+', 'buy_0-650'] + \
        [f'topup_{uid}' for uid, _ in (sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15).cursor().execute('SELECT user_id, username FROM users WHERE user_id != ?', (ADMIN_ID,)).fetchall() or [])]

    if data not in valid_callbacks:
        keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(texts[language]['invalid_callback'], reply_markup=reply_markup)
        context.user_data.clear()
        return

    if data.startswith('lang_'):
        lang = data[5:]
        with LOCK:
            conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15)
            try:
                c = conn.cursor()
                c.execute('UPDATE users SET language = ? WHERE user_id = ?', (lang, user_id))
                conn.commit()
            finally:
                conn.close()
        await query.edit_message_text(texts[lang]['welcome'])
        context.user_data.clear()
        await main_menu(update, context, user_id == ADMIN_ID)

    elif data == 'change_lang':
        keyboard = [
            [InlineKeyboardButton(texts['ru']['russian'], callback_data='lang_ru')],
            [InlineKeyboardButton(texts['en']['english'], callback_data='lang_en')],
            [InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(texts['ru']['choose_lang'], reply_markup=reply_markup)

    elif data == 'full_info':
        keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(texts[language]['full_info'], reply_markup=reply_markup)

    elif data == 'cat_young':
        keyboard = [
            [InlineKeyboardButton('2004', callback_data='buy_2004')],
            [InlineKeyboardButton('2005', callback_data='buy_2005')],
            [InlineKeyboardButton('2006', callback_data='buy_2006')],
            [InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(texts[language]['year'], reply_markup=reply_markup)

    elif data == 'cat_cs':
        keyboard = [
            [InlineKeyboardButton('CS&700+', callback_data='buy_700+')],
            [InlineKeyboardButton('CS0-650', callback_data='buy_0-650')],
            [InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(texts[language]['cs_type'], reply_markup=reply_markup)

    elif data == 'cat_states':
        keyboard = [
            [InlineKeyboardButton(state, callback_data=f'state_{state}') for state in STATES[i:i+2]]
            for i in range(0, len(STATES), 2)
        ]
        keyboard.append([InlineKeyboardButton(texts[language]['back'], callback_data='back_main')])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(texts[language]['state'], reply_markup=reply_markup)

    elif data == 'cat_dl':
        keyboard = [
            [InlineKeyboardButton(texts[language]['random_state'], callback_data='dl_random')],
            [InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(texts[language]['dl_quantity'], reply_markup=reply_markup)

    elif data == 'dl_random':
        keyboard = [
            [InlineKeyboardButton(texts[language]['back'], callback_data='cat_dl')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(texts[language]['dl_quantity'], reply_markup=reply_markup)
        context.user_data['state'] = 'awaiting_dl_quantity'

    elif data == 'dl_confirm':
        quantity = context.user_data.get('dl_quantity')
        if not quantity:
            keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='cat_dl')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(texts[language]['invalid_quantity'], reply_markup=reply_markup)
            context.user_data.clear()
            return
        await handle_purchase(query, context, user_id, language, 'DL_random', quantity)

    elif data.startswith('buy_') and (data.startswith('buy_200') or data.startswith('buy_700+') or data.startswith('buy_0-650') or any(data.startswith(f'buy_{state}_') for state in STATES)):
        subcategory = data[4:]
        if not any(subcategory == s or subcategory.startswith(f'{s}_') for s in ['2004', '2005', '2006', '700+', '0-650'] + STATES):
            keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(texts[language]['invalid_callback'], reply_markup=reply_markup)
            context.user_data.clear()
            return
        await handle_purchase(query, context, user_id, language, subcategory)

    elif data == 'back_main':
        context.user_data.clear()
        await main_menu(update, context, user_id == ADMIN_ID)

    elif data == 'buy_w2':
        keyboard = [
            [InlineKeyboardButton(texts[language]['dl_confirm_btn'], callback_data='buy_w2_confirm')],
            [InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(texts[language]['tax_description'], reply_markup=reply_markup)

    elif data == 'buy_w2_confirm':
        await handle_purchase(query, context, user_id, language, 'W2')

    elif data == 'topup':
        keyboard = [
            [InlineKeyboardButton('USDT TRC20', callback_data='crypto_USDT')],
            [InlineKeyboardButton('LTC', callback_data='crypto_LTC')],
            [InlineKeyboardButton('BTC', callback_data='crypto_BTC')],
            [InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(texts[language]['choose_crypto'], reply_markup=reply_markup)

    elif data == 'profile':
        with LOCK:
            conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15)
            try:
                c = conn.cursor()
                c.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
                row = c.fetchone()
                if row:
                    balance = row[0]
                    c.execute('SELECT product_name, price, quantity, timestamp FROM purchases WHERE user_id = ? ORDER BY timestamp DESC', (user_id,))
                    purchases = c.fetchall()
                    purchases_text = '\n'.join(
                        texts[language]['purchase_log'].format(name=name, price=price, quantity=quantity, timestamp=timestamp)
                        for name, price, quantity, timestamp in purchases
                    ) or texts[language]['no_purchases']
                    keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await query.edit_message_text(
                        texts[language]['profile_info'].format(user_id=user_id, balance=balance, purchases=purchases_text),
                        reply_markup=reply_markup
                    )
                else:
                    keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await query.edit_message_text(texts[language]['user_not_found'], reply_markup=reply_markup)
            finally:
                conn.close()

    elif data.startswith('crypto_'):
        crypto = data.split('_')[1]
        context.user_data['crypto'] = crypto
        context.user_data['state'] = 'awaiting_topup_amount'
        keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='topup')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(texts[language]['topup_prompt'], reply_markup=reply_markup)

    elif data == 'admin':
        if user_id == ADMIN_ID:
            keyboard = [
                [InlineKeyboardButton(texts[language]['add_product'], callback_data='add_product')],
                [InlineKeyboardButton(texts[language]['manual_topup'], callback_data='manual_topup')],
                [InlineKeyboardButton(texts[language]['view_keys'], callback_data='view_keys')],
                [InlineKeyboardButton(texts[language]['view_topups'], callback_data='view_topups')],
                [InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(texts[language]['admin_panel'], reply_markup=reply_markup)
        else:
            keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text("Access denied. Only admin can open admin panel.", reply_markup=reply_markup)

    elif data == 'add_product':
        if user_id == ADMIN_ID:
            keyboard = [
                [InlineKeyboardButton(texts[language]['back'], callback_data='admin')]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(texts[language]['add_product_prompt'], reply_markup=reply_markup)
            context.user_data['state'] = 'awaiting_product'
        else:
            keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text("Access denied. Only admin can add products.", reply_markup=reply_markup)

    elif data == 'manual_topup':
        if user_id == ADMIN_ID:
            with LOCK:
                conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15)
                try:
                    c = conn.cursor()
                    c.execute('SELECT user_id, username FROM users WHERE user_id != ?', (ADMIN_ID,))
                    users = c.fetchall()
                    keyboard = []
                    if users:
                        keyboard = [[InlineKeyboardButton(f"{uid} (@{uname if uname else 'None'})", callback_data=f'topup_{uid}') for uid, uname in users]]
                    keyboard.append([InlineKeyboardButton(texts[language]['back'], callback_data='admin')])
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await query.edit_message_text(texts[language]['select_user'] if users else texts[language]['no_users'], reply_markup=reply_markup)
                finally:
                    conn.close()
        else:
            keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text("Access denied. Only admin can top up balances.", reply_markup=reply_markup)

    elif data.startswith('topup_'):
        if user_id == ADMIN_ID:
            target_user_id = int(data.split('_')[1])
            keyboard = [
                [InlineKeyboardButton(texts[language]['back'], callback_data='manual_topup')]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(texts[language]['manual_topup_prompt'], reply_markup=reply_markup)
            context.user_data['state'] = 'awaiting_topup'
            context.user_data['target_user_id'] = target_user_id
        else:
            keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text("Access denied. Only admin can perform this action.", reply_markup=reply_markup)

    elif data == 'view_keys':
        if user_id == ADMIN_ID:
            with LOCK:
                conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15)
                try:
                    c = conn.cursor()
                    c.execute('SELECT user_id FROM users')
                    keys_info = ''
                    for row in c.fetchall():
                        user_id = row[0]
                        keys_info += texts[language]['keys_info'].format(user_id=user_id, addr_btc='N/A', addr_ltc='N/A', addr_usdt='N/A')
                    keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='admin')]]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await query.edit_message_text(keys_info or "No keys found.", reply_markup=reply_markup)
                finally:
                    conn.close()
        else:
            keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text("Access denied. Only admin can view keys.", reply_markup=reply_markup)

    elif data == 'view_topups':
        if user_id == ADMIN_ID:
            with LOCK:
                conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15)
                try:
                    c = conn.cursor()
                    c.execute('SELECT t.user_id, t.amount, t.timestamp, t.crypto, u.first_name, u.last_name, u.username FROM topups t JOIN users u ON t.user_id = u.user_id ORDER BY t.timestamp DESC')
                    topups_info = ''
                    for row in c.fetchall():
                        user_id, amount, timestamp, crypto, first_name, last_name, username = row
                        username = username if username else 'None'
                        crypto = crypto if crypto else 'Manual'
                        topups_info += texts[language]['topup_log'].format(user_id=user_id, first_name=first_name, last_name=last_name, username=username, amount=amount, crypto=crypto, timestamp=timestamp)
                    keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='admin')]]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await query.edit_message_text(topups_info or 'No top-ups found.', reply_markup=reply_markup)
                finally:
                    conn.close()
        else:
            keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='back_main')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text("Access denied. Only admin can view top-ups.", reply_markup=reply_markup)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    message_text = update.message.text
    language = get_user_language(user_id) or 'ru'

    if context.user_data.get('state') == 'awaiting_product' and user_id == ADMIN_ID:
        try:
            parts = message_text.split('|')
            if len(parts) != 6:
                raise ValueError
            name, price, stock, file_path, category, subcategory = [p.strip() for p in parts]
            price = float(price)
            stock = int(stock)
            with LOCK:
                conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15)
                try:
                    c = conn.cursor()
                    c.execute('INSERT INTO products (name, price, stock, file_path, category, subcategory) VALUES (?, ?, ?, ?, ?, ?)',
                            (name, price, stock, file_path, category, subcategory))
                    conn.commit()
                    keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='admin')]]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await update.message.reply_text(texts[language]['product_added'], reply_markup=reply_markup)
                finally:
                    conn.close()
            context.user_data.clear()
        except ValueError:
            keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='admin')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(texts[language]['format_error'], reply_markup=reply_markup)
            context.user_data.clear()

    elif context.user_data.get('state') == 'awaiting_topup' and user_id == ADMIN_ID:
        try:
            amount = float(message_text)
            target_user_id = context.user_data.get('target_user_id')
            if not target_user_id:
                keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='manual_topup')]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await update.message.reply_text(texts[language]['invalid_quantity'], reply_markup=reply_markup)
                context.user_data.clear()
                return
            with LOCK:
                conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15)
                try:
                    c = conn.cursor()
                    c.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (amount, target_user_id))
                    c.execute('INSERT INTO topups (user_id, amount, timestamp, status, crypto) VALUES (?, ?, ?, ?, ?)',
                            (target_user_id, amount, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'completed', 'Manual'))
                    conn.commit()
                    keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='admin')]]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await update.message.reply_text(texts[language]['balance_topup'].format(tgt_user_id=target_user_id, amount=amount), reply_markup=reply_markup)
                    target_lang = get_user_language(target_user_id) or 'ru'
                    await context.bot.send_message(target_user_id, texts[target_lang]['balance_updated'].format(amount=amount))
                finally:
                    conn.close()
            context.user_data.clear()
        except ValueError:
            keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='manual_topup')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(texts[language]['format_error'], reply_markup=reply_markup)
            context.user_data.clear()

    elif context.user_data.get('state') == 'awaiting_dl_quantity':
        try:
            quantity = int(message_text)
            if quantity <= 0:
                raise ValueError
            with LOCK:
                conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15)
                try:
                    c = conn.cursor()
                    c.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
                    balance = c.fetchone()[0]
                finally:
                    conn.close()
            total_price = 15.0 * quantity
            if balance >= total_price:
                context.user_data['dl_quantity'] = quantity
                keyboard = [
                    [InlineKeyboardButton(texts[language]['dl_confirm_btn'], callback_data='dl_confirm')],
                    [InlineKeyboardButton(texts[language]['back'], callback_data='cat_dl')]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await update.message.reply_text(
                    texts[language]['dl_confirm'].format(quantity=quantity, total=total_price, balance=balance),
                    reply_markup=reply_markup
                )
                context.user_data['state'] = 'awaiting_dl_confirm'
            else:
                keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='cat_dl')]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await update.message.reply_text(texts[language]['no_funds'], reply_markup=reply_markup)
                context.user_data.clear()
        except ValueError:
            keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='cat_dl')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(texts[language]['invalid_quantity'], reply_markup=reply_markup)
            context.user_data.clear()

    elif context.user_data.get('state') == 'awaiting_topup_amount':
        try:
            usd_amount = float(message_text)
            if usd_amount <= 0:
                raise ValueError
            crypto = context.user_data.get('crypto')
            if not crypto:
                keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='topup')]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await update.message.reply_text(texts[language]['invalid_quantity'], reply_markup=reply_markup)
                context.user_data.clear()
                return
            pay_url, invoice_id, error = await create_crypto_invoice(user_id, usd_amount, crypto, context)
            if pay_url and invoice_id:
                with LOCK:
                    conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=15)
                    try:
                        c = conn.cursor()
                        c.execute('INSERT INTO topups (user_id, amount, timestamp, invoice_id, status, crypto) VALUES (?, ?, ?, ?, ?, ?)',
                                (user_id, usd_amount, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), invoice_id, 'pending', crypto))
                        conn.commit()
                    finally:
                        conn.close()
                exchange_rate = await get_exchange_rate(crypto)
                crypto_amount = usd_amount / exchange_rate if exchange_rate else usd_amount
                keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='topup')]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await update.message.reply_text(texts[language]['topup_invoice'].format(amount=f"{crypto_amount:.8f}", crypto=crypto, usd_amount=usd_amount, invoice_url=pay_url), reply_markup=reply_markup)
                logging.info(f"Created invoice {invoice_id} for user_id {user_id} for {usd_amount} USD ({crypto_amount:.8f} {crypto})")
            else:
                keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='topup')]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await update.message.reply_text(texts[language]['topup_failed'].format(error=error or 'Unable to create invoice'), reply_markup=reply_markup)
            context.user_data.clear()
        except ValueError:
            keyboard = [[InlineKeyboardButton(texts[language]['back'], callback_data='topup')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(texts[language]['invalid_quantity'], reply_markup=reply_markup)
            context.user_data.clear()

async def main():
    try:
        if not await check_crypto_token():
            raise Exception("Invalid @CryptoBot token. Please check CRYPTO_TOKEN.")
        
        init_db_and_folders()
        application = Application.builder().token(BOT_TOKEN).read_timeout(15).connect_timeout(15).build()
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CallbackQueryHandler(button))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        application.job_queue.run_once(init_products, when=5)
        application.job_queue.run_repeating(check_pending_invoices, interval=60, first=10)
        await application.initialize()
        await application.start()
        await application.updater.start_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True, poll_interval=1.0, timeout=30)
        logging.info("Polling started, bot is running...")
        while True:
            await asyncio.sleep(3600)
    except Exception as e:
        logging.error(f"Error starting bot: {e}")
        raise
    finally:
        logging.info("Shutting down application...")
        try:
            await application.updater.stop()
            await application.stop()
            await application.shutdown()
        except Exception as e:
            logging.error(f"Error shutting down application: {e}")

if __name__ == '__main__':
    import asyncio
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot stopped by user")
    except Exception as e:
        logging.error(f"Critical error in asyncio.run: {e}")