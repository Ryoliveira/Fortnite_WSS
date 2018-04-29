#! python3
# Created: 4/11/2018
'''This program scrapes weapon stats from url: http://orcz.com/Fortnite_Battle_Royale:_Weapons
   These stats are stored in a database with a table for each weapon'''
import requests
from bs4 import BeautifulSoup
import sqlite3
import sys
import time

# Weapon_name, (StartIndex, StopIndex), slots
WEAPON_GROUPS = {
                'AssaultRifle':(0, 10, 10), 'Sniper':(10, 17, 10), 'ShotGun':(17, 24, 9), 'SubmachineGun':(24, 30, 10),
                'MiniGun':(30, 34, 10), 'Pistol':(34, 44, 10), 'CrossBow':(44, 46, 10), 'RocketLauncher':(46, 49, 9),
                'GrenadeLauncher':(49, 52, 9), 'Grenade':(52, 56, 7), 'Other':(57, 58, 4)
                }


def parse_tables(soup):
    """Parse through table and grab weapon data"""
    weapons = []
    for table in soup.find_all('table', class_="wikitable")[1:-1]:
        for tr in table.find_all('tr'):
            stats = []  # Stats for current weapon
            for td in tr.find_all("td"):
                if td.text != ' ' and check(td.text.strip()):
                    stats.append(td.text.strip())
            if len(stats) > 1:
                weapons.append(stats)
    return weapons


def check(text):
    """Check whether text holds any of these flags"""
    if text.startswith('egg'):
        return False
    if text in [' ', '0%', '???']:
        return False
    return True


def grab_page():
    """Pull Weapon Stat info
       Returns list of weapons and corresponding stats"""
    connection_error = True
    retry = 0
    while connection_error:
        try:
            r = requests.get('http://orcz.com/Fortnite_Battle_Royale:_Weapons')
            soup = BeautifulSoup(r.content, 'lxml')
            return soup
        except requests.exceptions.RequestException as e:
            print(e)
            print("Error with internet connection."
                  "\nCheck connection and try again.")
            retry += 1
            if retry > 5:
                print("Error: Max retries exceeded")
                sys.exit(1)
            for i in range(-15, 1):
                if i % 5 == 0:
                    print("Auto Retry in {} Seconds...".format(abs(i)))
                time.sleep(1)


def choose_slots(slot, name):
    """Changes slot content for appropriate weapon"""
    if slot == 10:
        slot = '''Weapon TEXT, Rarity TEXT, Dps TEXT, Damage TEXT,FireRate TEXT, MagSize TEXT, ReloadTime TEXT,
                  AmmoCost TEXT, HeadShotDamage TEXT, StructureDamage TEXT'''
    if slot == 9:
        slot = '''Weapon TEXT, Rarity TEXT, Dps TEXT, Damage TEXT, FireRate TEXT, MagSize TEXT, ReloadTime TEXT,
                  AmmoCost TEXT, StructureDamage TEXT'''
    if name == 'Grenade':
        slot = '''Weapon TEXT, Rarity TEXT, Dps TEXT, Damage TEXT, CritChance TEXT, CritDamage TEXT, StructureDamage TEXT'''
    if name == 'ShotGun':
        slot = '''Weapon TEXT, Rarity TEXT, Dps TEXT, Damage TEXT, FireRate TEXT, MagSize TEXT, ReloadTime TEXT,
                  HeadShotDamage TEXT, StructureDamage TEXT'''
    if name == 'Other':
        slot = ''' Weapon TEXT, Rarity TEXT, Damage TEXT, StructureDamage TEXT'''
    return slot


def create_empty_table(name, slot, cur):
    """Creates empty table for weapon type"""
    current_slot = choose_slots(slot, name)
    cur.execute("DROP TABLE IF EXISTS {}".format(name))
    cur.execute('''CREATE TABLE {}({})'''.format(name, current_slot))


def data_insert(weapon_data, table_name, slot, cur):
    if table_name =='Other':
        cur.execute('''INSERT INTO {} (Weapon, Rarity, Damage, StructureDamage)
                                           VALUES (?, ?, ?, ?)'''.format(table_name), (*weapon_data,))
    if table_name == 'Grenade':
        cur.execute('''INSERT INTO {} (Weapon, Rarity, Dps, Damage, CritChance, CritDamage, StructureDamage)
                                           VALUES (?, ?, ?, ?, ?, ?, ?)'''.format(table_name), (*weapon_data[:-1],))
    if table_name == 'ShotGun':
        cur.execute('''INSERT INTO {} (Weapon, Rarity, Dps, Damage, FireRate, MagSize,
                                           ReloadTime, HeadShotDamage, StructureDamage)
                                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'''.format(table_name), (*weapon_data,))
    elif slot == 10:
        cur.execute('''INSERT INTO {} (Weapon, Rarity, Dps, Damage, FireRate, MagSize,
                                           ReloadTime, AmmoCost, HeadShotDamage, StructureDamage)
                                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''.format(table_name), (*weapon_data,))
    elif slot == 9:
        cur.execute('''INSERT INTO {} (Weapon, Rarity, Dps, Damage, FireRate, MagSize,
                                               ReloadTime, AmmoCost, StructureDamage)
                                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'''.format(table_name), (*weapon_data,))


def create_database(weapons, cur):
    """Create Database with weapon stats provided from website"""
    for weapon_name, (start, stop, slot) in WEAPON_GROUPS.items():
        create_empty_table(weapon_name, slot, cur)
        for weapon in weapons[start:stop]:
            data_insert(weapon, weapon_name, slot, cur)


def main():
    con = sqlite3.connect('Weapons.DB')
    cur = con.cursor()
    print('Fetching Data...')
    soup = grab_page()
    print('Creating database...')
    weapons = parse_tables(soup)
    create_database(weapons, cur)
    print("Database Created!")
    con.commit()
    con.close()

if __name__ == '__main__':
    main()
