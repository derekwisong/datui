# Food nutrition (fast food): the five steps of the Sort, filter and arrange
# columns page, the sidebar reopened on Columns over the 30 rows it left; then
# the restaurant summary in the Copy dialog, set to Table and Markdown and not
# copied. Pages: user-guide/filtering-sorting.md, user-guide/copying.md.

Hide
Type "clear; datui"
Enter
Wait+Screen /EXAMPLE DATASETS/
Show

Type "food"
Wait+Screen /1 match/
Enter
Wait+Screen /1 \/ 515/
Sleep 500ms

# 1. Keep the rows with the letters of "chicken" in order.
Type "/"
Sleep 300ms
Ctrl+T
Type "chicken"
Sleep 500ms
Ctrl+G
Wait+Screen /1 \/ 178/

# 2. protein >= 40. The find's filter is a row of its own, so two steps down.
Type "s"
Sleep 600ms
Down 2
Space
Sleep 300ms
Type "protein"
Enter
Type ">="
Enter
Type "40"
Enter
Sleep 300ms

# 3. Sort by calories, descending.
Up 3
Space
Sleep 300ms
Type "calories"
Enter
Sleep 300ms
Space
Sleep 300ms

# 4. Hide vit_a and vit_c.
Up
Right
Down
Type "vit"
Sleep 300ms
Down
Type "v"
Down
Type "v"
Sleep 300ms

# 5. Apply: 30 of 515, 2,430 calories first.
Enter
Wait+Screen /1 \/ 30 /
Sleep 500ms

# The sidebar again, on Columns, the table behind it.
Type "s"
Sleep 600ms
Up
Right
Down
Type "vit"
Wait+Screen /vit_c ⊘/
Sleep 1s
Screenshot sort-filter-columns.png
Escape
Sleep 300ms
Escape
Sleep 300ms

# The restaurant summary, ready to copy as Markdown.
Type ":"
Sleep 300ms
Ctrl+U
Type@15ms "SELECT restaurant, ROUND(AVG(calories), 0) AS avg_calories, ROUND(AVG(protein), 1) AS avg_protein, COUNT(*) AS items FROM df GROUP BY restaurant ORDER BY avg_calories DESC"
Enter
Wait+Screen /1 \/ 8 /
Type "y"
Sleep 500ms
Right 2
Down
Right 2
Wait+Screen /Copy all 8 rows as Markdown/
Sleep 1s
Screenshot copy-markdown.png
