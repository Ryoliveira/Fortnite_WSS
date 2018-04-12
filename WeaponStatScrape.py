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
    cur.execute("DROP TABLE IF EXISTS AssaultRifle")
    cur.execute('''CREATE TABLE AssaultRifle({})'''.format(slot10))

    for assault_rifle in weapons[:10]:
        cur.execute('''INSERT INTO AssaultRifle(Weapon, Rarity, Dps, Damage, FireRate, MagSize, 
                                           ReloadTime, AmmoCost, HeadShotDamage, StructureDamage)
                                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (*assault_rifle,))
    # Sniper
    # Column: 10
    cur.execute("DROP TABLE IF EXISTS Sniper")
    cur.execute('''CREATE TABLE Sniper({})'''.format(slot10))

    for sniper in weapons[10:17]:
        cur.execute('''INSERT INTO Sniper (Weapon, Rarity, Dps, Damage, FireRate, MagSize, 
                                           ReloadTime, AmmoCost, HeadShotDamage, StructureDamage)
                                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (*sniper,))
    # SMG
    # Column: 10
    cur.execute("DROP TABLE IF EXISTS SubmachineGun")
    cur.execute('''CREATE TABLE SubmachineGun({})'''.format(slot10))

    for smg in weapons[24:30]:
        cur.execute('''INSERT INTO SubmachineGun (Weapon, Rarity, Dps, Damage, FireRate, MagSize, 
                                           ReloadTime, AmmoCost, HeadShotDamage, StructureDamage)
                                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (*smg,))
    # Pistol
    # Column: 10
    cur.execute("DROP TABLE IF EXISTS Pistol")
    cur.execute('''CREATE TABLE Pistol({})'''.format(slot10))

    for pistol in weapons[32:42]:
        cur.execute('''INSERT INTO Pistol (Weapon, Rarity, Dps, Damage, FireRate, MagSize, 
                                           ReloadTime, AmmoCost, HeadShotDamage, StructureDamage)
                                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (*pistol,))
    # CrossBow
    # Column: 10
    cur.execute("DROP TABLE IF EXISTS CrossBow")
    cur.execute('''CREATE TABLE CrossBow({})'''.format(slot10))

    for crossbow in weapons[42:44]:
        cur.execute('''INSERT INTO CrossBow (Weapon, Rarity, Dps, Damage, FireRate, MagSize, 
                                           ReloadTime, AmmoCost, HeadShotDamage, StructureDamage)
                                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (*crossbow,))
    # MiniGun
    # Column: 10
    cur.execute("DROP TABLE IF EXISTS MiniGun")
    cur.execute('''CREATE TABLE MiniGun({})'''.format(slot10))

    for minigun in weapons[30:31]:
        cur.execute('''INSERT INTO MiniGun (Weapon, Rarity, Dps, Damage, FireRate, MagSize, 
                                           ReloadTime, AmmoCost, HeadShotDamage, StructureDamage)
                                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (*minigun,))
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
    cur.execute("DROP TABLE IF EXISTS RocketLauncher")
    cur.execute('''CREATE TABLE RocketLauncher({})'''.format(slot9))

    for rocket_launcher in weapons[44:49]:
        cur.execute('''INSERT INTO RocketLauncher (Weapon, Rarity, Dps, Damage, FireRate, MagSize, 
                                           ReloadTime, AmmoCost, StructureDamage)
                                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', (*rocket_launcher,))
    # Grenade Launcher
    # Column: 9
    cur.execute("DROP TABLE IF EXISTS GrenadeLauncher")
    cur.execute('''CREATE TABLE GrenadeLauncher({})'''.format(slot9))

    for grenade_launcher in weapons[49:52]:
        cur.execute('''INSERT INTO GrenadeLauncher (Weapon, Rarity, Dps, Damage, FireRate, MagSize, 
                                           ReloadTime, AmmoCost, StructureDamage)
                                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', (*grenade_launcher,))

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
