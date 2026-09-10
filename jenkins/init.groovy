// jenkins/init.groovy
// Runs once on first boot (empty jenkins_home volume).
// Creates the 'admin' user from JENKINS_ADMIN_PASSWORD env var,
// enables full security (login required), and grants admin all permissions.
// On subsequent boots the user already exists — the script is a safe no-op.

import jenkins.model.*
import hudson.security.*
import jenkins.install.*

def instance = Jenkins.get()

// Only run if security has not been set up yet (fresh volume).
def realm = instance.getSecurityRealm()
if (realm instanceof HudsonPrivateSecurityRealm) {
    def users = realm.getAllUsers()
    if (!users.isEmpty()) {
        println "[init-admin-user] Users already exist — skipping."
        return
    }
}

def password = System.getenv("JENKINS_ADMIN_PASSWORD") ?: "changeme"

def newRealm = new HudsonPrivateSecurityRealm(false)
newRealm.createAccount("admin", password)
instance.setSecurityRealm(newRealm)

def strategy = new FullControlOnceLoggedInAuthorizationStrategy()
strategy.setAllowAnonymousRead(false)
instance.setAuthorizationStrategy(strategy)

// Mark setup as complete so the wizard never appears
instance.setInstallState(InstallState.INITIAL_SETUP_COMPLETED)

instance.save()
println "[init-admin-user] Admin user created. Jenkins is ready."
