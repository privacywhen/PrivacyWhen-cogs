class User:
    def __init__(self, name, role, course, channel):
        self.name = name
        self.role = role
        self.course = course
        self.channel = channel
    
#Auto-scrape???

profile = User("Nick", "Alien", "Phys 4G03", "Phys 4G03")
database = []
pattern = upper() #Pass through data to enforce upper case only

for profile.name in database: #Join
    if profile.role == True:
        if channel == True:
            if profile.role in User == True:
                print("User already has course access")
            else:
                database.add[profile.name, profile.role, profile.course]
        else:
            database.add[profile.channel]
    else:
        if profile.course == pattern:
            database.add[profile.role]
        if profile.course != pattern:
            print("Invalid Course Code")
    continue

for User in database: #Leave
    if profile.name == True:
        database.remove[profile.name, profile.role, profile.course, profile.channel]
    else:
        print("User does not have role to remove")