# Firebase sync setup (one time, ~10 minutes)

This is what makes a reminder you add on **one** device appear and ring on **both**.
You do this once. It's free (Firebase's free "Spark" plan is far more than enough
for personal reminders). No credit card.

You'll end up with two things to type into Prism's **Settings → Cross-device sync**:

1. a **Database URL**
2. a **sync code** (you make this up / generate it in the app)

---

## Step 1 — Create a Firebase project

1. Go to **https://console.firebase.google.com** and sign in with your Google account.
2. Click **Add project** (or **Create a project**).
3. Name it anything, e.g. `prism-reminders`. Click **Continue**.
4. On the "Google Analytics" step, toggle it **off** (you don't need it). Click **Create project**.
5. Wait ~30 seconds, then click **Continue**.

## Step 2 — Turn on the Realtime Database

1. In the left sidebar, open **Build → Realtime Database**.
   *(Make sure it's "Realtime Database", not "Firestore".)*
2. Click **Create Database**.
3. Pick the location closest to you (e.g. *United States* or *Europe*). Click **Next**.
4. Choose **Start in test mode**. Click **Enable**.
5. You'll now see your database. Copy the **URL** at the top — it looks like one of:
   - `https://prism-reminders-default-rtdb.firebaseio.com`
   - `https://prism-reminders-default-rtdb.europe-west1.firebasedatabase.app`

   **That URL is the first thing you paste into Prism.**

## Step 3 — Paste the rules (so it doesn't lock you out in 30 days)

"Test mode" expires after 30 days. Replace its rules with the ones below, which
keep the database closed **except** for reminder "rooms" — and a room is only
reachable by someone who knows your long random sync code.

1. In **Realtime Database**, click the **Rules** tab.
2. Delete what's there and paste this exactly:

   ```json
   {
     "rules": {
       "rooms": {
         "$code": {
           ".read": true,
           ".write": true
         }
       }
     }
   }
   ```

3. Click **Publish**.

> **What this means, honestly:** anyone who knows your sync code can read/write
> that one room. Nobody can list rooms or touch anything else. With a 14-character
> random code that's ~72 bits of guess-resistance — fine for personal reminders.
> If you ever want stronger (signed-in) security, that's a later upgrade; tell me
> and I'll wire it in.

## Step 4 — Connect both devices in the app

On the **first** device (say your Mac):

1. Open Prism → tap the **⚙ gear** → **Cross-device sync**.
2. Paste the **Database URL** from Step 2.
3. Tap **Generate code** (or type your own long code).
4. Tap **Save sync settings**.

On the **second** device (your Vivo):

1. Open Prism → **⚙ gear** → **Cross-device sync**.
2. Paste the **same Database URL**.
3. Type the **same sync code** (exactly — it's case-sensitive).
4. Tap **Save sync settings**.

Done. Add a reminder on either one; within ~12 seconds it shows up on the other,
and both devices ring at the scheduled time.

---

## Quick check it's working

- On the Mac, add a reminder for a couple of minutes out.
- Watch the Vivo — it should appear in the list shortly.
- At the set time, both buzz/ring.

If it doesn't appear on the second device, 99% of the time it's one of:
- the **URL** differs (trailing slash is fine; a typo in the subdomain is not),
- the **sync code** differs (capital vs lowercase counts),
- the device is **offline**.
