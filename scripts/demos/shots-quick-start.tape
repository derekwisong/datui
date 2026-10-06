# Quick start on Palmer penguins: the home screen narrowed to it, the species
# summary in SQL, and a scatter of flipper length against body mass.
# Page: getting-started/quick-start.md (sections 1 to 3).

Hide
Type "clear; datui"
Enter
Wait+Screen /EXAMPLE DATASETS/
Show

# 1. Narrowed to Palmer penguins, its details beside it.
Type "penguins"
Wait+Screen /1 match/
Sleep 1s
Screenshot quick-start-home.png

# 2. Which species is heaviest?
Enter
Wait+Screen /1 \/ 344/
Sleep 500ms
Type ":"
Sleep 300ms
Type@15ms "SELECT species, AVG(body_mass_g) AS mean_mass_g, COUNT(*) AS penguins FROM df GROUP BY species ORDER BY mean_mass_g DESC"
Enter
Wait+Screen /1 \/ 3 /
Sleep 1s
Screenshot quick-start-sql.png

# 3. Back to the rows; a scatter, Y from the cursor's column.
Type "R"
Wait+Screen /1 \/ 344/
Type "g"
Sleep 200ms
Type "body_mass_g"
Enter
Sleep 300ms
Type "c2"
Sleep 800ms
Down
Space
Sleep 200ms
Type "flipper"
Sleep 300ms
Enter
Wait+Screen /flipper_length_mm/
Sleep 1.5s
Screenshot quick-start-chart.png
