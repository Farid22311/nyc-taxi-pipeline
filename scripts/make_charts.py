import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

OUT = "docs/images"

# Chart 1: trips per hour (q01)
q01 = pd.read_csv("output/q01.csv")
print("hour 18:", q01.loc[q01.pickup_hour == 18, "trips"].item())
print("hour 4:", q01.loc[q01.pickup_hour == 4, "trips"].item())

fig, ax = plt.subplots(figsize=(9, 4.5))
ax.bar(q01.pickup_hour, q01.trips)
ax.set_xlabel("Pickup hour")
ax.set_ylabel("Trips")
ax.set_title("Trips per hour, August 2026 (reversals excluded)")
ax.set_xticks(range(24))
fig.tight_layout()
fig.savefig(OUT + "/trips_per_hour.png", dpi=150)
plt.close(fig)

# Chart 2: daily revenue (q05), without Jul 31 (a partial day)
q05 = pd.read_csv("output/q05.csv", parse_dates=["pickup_date"])
q05 = q05[q05.pickup_date >= "2026-08-01"]
spike = q05.loc[q05.pickup_date == "2026-08-20"].iloc[0]
print("Aug 20 revenue:", spike.revenue)

fig, ax = plt.subplots(figsize=(9, 4.5))
ax.plot(q05.pickup_date, q05.revenue / 1e6, marker="o")
ax.annotate("Aug 20: $%.2fM" % (spike.revenue / 1e6),
            xy=(spike.pickup_date, spike.revenue / 1e6),
            xytext=(10, 10), textcoords="offset points")
ax.set_xlabel("Pickup date")
ax.set_ylabel("Revenue (million USD)")
ax.set_title("Daily revenue, August 2026 (reversals excluded)")
fig.autofmt_xdate()
fig.tight_layout()
fig.savefig(OUT + "/daily_revenue.png", dpi=150)
plt.close(fig)
