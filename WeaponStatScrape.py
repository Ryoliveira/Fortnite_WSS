# Created: 4/11/2018
'''This program scrapes weapon stats from url: http://orcz.com/Fortnite_Battle_Royale:_Weapons
   These stats are stored in a database with a table for each weapon'''
import requests
from bs4 import BeautifulSoup
import sqlite3
import sys
import time


def grab_tables():
    """Pull Weapon Stat info
       Returns list of weapons and corresponding stats"""
    connection_error = True
    retry = 0
    while connection_error:
        try:
            r = requests.get('http://orcz.com/Fortnite_Battle_Royale:_Weapons')
            soup = BeautifulSoup(r.content, 'lxml')
            weapons = []  # Each row is a different weapon with corresponding stats
            for table in soup.find_all('table', class_="wikitable")[1:-1]:
                for tr in table.find_all('tr'):
                    stats = []  # Stats for current weapon
                    for td in tr.find_all("td"):
                        if td.text != ' ' and td.text.strip() != '0%' \
                                and td.text.strip() != '???' and not td.text.strip().startswith("egg"):
                            stats.append(td.text.strip())
                    if len(stats) > 1:
                        weapons.append(stats)
            return weapons
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


def data_insert(slot, data, cur, table_name):
    if slot == 10:
        cur.execute('''INSERT INTO {} (Weapon, Rarity, Dps, Damage, FireRate, MagSize, 
                                           ReloadTime, AmmoCost, HeadShotDamage, StructureDamage)
                                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''.format(table_name), (*data,))
    if slot == 9:
        cur.execute('''INSERT INTO {} (Weapon, Rarity, Dps, Damage, FireRate, MagSize, 
                                               ReloadTime, AmmoCost, StructureDamage)
                                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'''.format(table_name), (*data,))


def create_empty_table(slot, name, cur):
    cur.execute("DROP TABLE IF EXISTS {}".format(name))
    cur.execute('''CREATE TABLE {}({})'''.format(name, slot))


def create_database(weapons):
    """Create Database with weapon stats provided from website"""
    con = sqlite3.connect('Weapons.DB')
    cur = con.cursor()

    slot10 = '''Weapon TEXT,
                Rarity TEXT,
                Dps TEXT,
                Damage TEXT,
                FireRate TEXT,
                MagSize TEXT,
                ReloadTime TEXT,
                AmmoCost TEXT,
                HeadShotDamage TEXT,
                StructureDamage TEXT'''
    # Assault Rifles
    # Column: 10
    table_name = 'AssaultRifle'
    create_empty_table(slot10, table_name, cur)
    for assault_rifle in weapons[:10]:
        data_insert(10, assault_rifle, cur, table_name)
    # Sniper
    # Column: 10
    table_name = 'Sniper'
    create_empty_table(slot10, table_name, cur)
    for sniper in weapons[10:17]:
        data_insert(10, sniper, cur, table_name)
    # SMG
    # Column: 10
    table_name = 'SubmachineGun'
    create_empty_table(slot10, table_name, cur)
    for smg in weapons[24:30]:
        data_insert(10, smg, cur, table_name)
    # Pistol
    # Column: 10
    table_name = 'Pistol'
    create_empty_table(slot10, table_name, cur)

    for pistol in weapons[32:42]:
        data_insert(10, pistol, cur, table_name)
    # CrossBow
    # Column: 10
    table_name = 'CrossBow'
    create_empty_table(slot10, table_name, cur)

    for crossbow in weapons[42:44]:
        data_insert(10, crossbow, cur, table_name)
    # MiniGun
    # Column: 10
    table_name = 'MiniGun'
    create_empty_table(slot10, table_name, cur)

    for minigun in weapons[30:31]:
        data_insert(10, minigun, cur, table_name)
    # Shotgun
    # Column: 9
    cur.execute("DROP TABLE IF EXISTS Shotgun")
    cur.execute('''CREATE TABLE Shotgun(
                Weapon TEXT,
                Rarity TEXT,
                Dps TEXT,
                Damage TEXT,
                FireRate TEXT,
                MagSize TEXT,
                ReloadTime TEXT,
                HeadShotDamage TEXT,
                StructureDamage TEXT
                )''')

    for shotgun in weapons[17:24]:
        cur.execute('''INSERT INTO Shotgun (Weapon, Rarity, Dps, Damage, FireRate, MagSize, 
                                           ReloadTime, HeadShotDamage, StructureDamage)
                                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', (*shotgun,))
    # For explosive entries with 9 columns
    slot9 = '''Weapon TEXT,
                Rarity TEXT,
                Dps TEXT,
                Damage TEXT,
                FireRate TEXT,
                MagSize TEXT,
                ReloadTime TEXT,
                AmmoCost TEXT,
                StructureDamage TEXT'''
    # Rocket Launcher
    # Column: 9
    table_name = 'RocketLauncher'
    create_empty_table(slot9, table_name, cur)
    for rocket_launcher in weapons[44:49]:
        data_insert(9, rocket_launcher, cur, table_name)
    # Grenade Launcher
    # Column: 9
    table_name = 'GrenadeLauncher'
    create_empty_table(slot9, table_name, cur)
    for grenade_launcher in weapons[49:52]:
        data_insert(9, grenade_launcher, cur, table_name='GrenadeLauncher')

    # Grenade
    # Column: 7
    cur.execute("DROP TABLE IF EXISTS Grenade")
    cur.execute('''CREATE TABLE Grenade(
                Weapon TEXT,
                Rarity TEXT,
                Dps TEXT,
                Damage TEXT,
                CritChance TEXT,
                CritDamage TEXT,
                StructureDamage TEXT
                )''')

    for grenade in weapons[52:55]:
        cur.execute('''INSERT INTO Grenade (Weapon, Rarity, Dps, Damage, CritChance, CritDamage, StructureDamage)
                                           VALUES (?, ?, ?, ?, ?, ?, ?)''', (*grenade,))

    # Other
    # Column: 4
    cur.execute("DROP TABLE IF EXISTS Other")
    cur.execute('''CREATE TABLE Other(
                Weapon TEXT,
                Rarity TEXT,
                Damage TEXT,
                StructureDamage TEXT
                )''')

    for other in weapons[55:56]:
        cur.execute('''INSERT INTO Other (Weapon, Rarity, Damage, StructureDamage)
                                           VALUES (?, ?, ?, ?)''', (*other,))

    con.commit()
    con.close()


if __name__ == '__main__':
    weapons = grab_tables()
    create_database(weapons)
