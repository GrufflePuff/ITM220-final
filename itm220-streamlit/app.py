import streamlit as st
import pandas as pd
import mysql.connector
import os
import hashlib
from mysql.connector import Error
from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv
load_dotenv()

mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_password_local = os.getenv("MYSQL_PASSWORD_LOCAL")

# requirement queries:

queries = {
    "function": """
        SELECT
        g.id,
        g.game_name,
        COUNT(r.recommended) AS total_reviews,            
        COALESCE(SUM(r.recommended), 0) AS positive_reviews,
        COUNT(r.recommended) - COALESCE(SUM(r.recommended), 0) AS negative_reviews
        FROM games AS g
        LEFT JOIN reviews AS r
        ON r.game_id = g.id
        GROUP BY g.id, g.game_name
        ORDER BY g.game_name;""",
    "inner join": """
        SELECT g.game_name AS `game`, d.developer_name AS `developer`, g.release_date AS `release`, g.price AS `price`
        FROM games AS g
        JOIN developers AS d
        ON g.developers_id = d.id""",
    "conditional logic": """
        SELECT game_name,
        CASE
        WHEN price = 0 THEN 'Free'
        ELSE price
        END AS price_category
        FROM games;""",
    "outer join": """
        SELECT g.game_name
        FROM games AS g
        LEFT JOIN library AS l
        ON g.id = l.games_id AND l.users_id = 1
        WHERE l.games_id IS NULL;""",
    "aggregate function and GROUP BY": """
        SELECT g.game_name,
        CASE
        WHEN AVG(r.recommended) * 100 >= 80 THEN 'Very Positive'
        WHEN AVG(r.recommended) * 100 >= 60 THEN 'Positive'
        WHEN AVG(r.recommended) * 100 >= 40 THEN 'Mixed'
        WHEN AVG(r.recommended) * 100 >= 20 THEN 'Negative'
        WHEN AVG(r.recommended) * 100 < 20 THEN 'Very Negative'
        END AS rating
        FROM reviews AS r
        JOIN games AS g
        ON g.id = r.game_id
        GROUP BY g.id, g.game_name;""",
    "subquery": """
        SELECT g.game_name,
        (SELECT GROUP_CONCAT(t.name SEPARATOR ', ')
        FROM game_tags gt
        JOIN tag t ON gt.tag_id = t.id
        WHERE gt.games_id = g.id) AS tags
        FROM games g;""",
    "window function": """
        SELECT 
        g.game_name,
        COUNT(r.id) AS total_reviews,
        RANK() OVER (ORDER BY AVG(r.recommended) DESC) AS rank_by_rating
        FROM games g
        LEFT JOIN reviews r ON r.game_id = g.id
        GROUP BY g.id, g.game_name
        ORDER BY rank_by_rating;
        """
}


# Set up SSH tunnel


# ---------- Database Connection ----------
def get_connection():
    try:
        server = SSHTunnelForwarder(
        (st.secrets["ssh"]["ssh_host"], 22),
        ssh_username=st.secrets["ssh"]["ssh_user"],
        ssh_pkey=st.secrets["ssh"]["ssh_pem_path"],
        remote_bind_address=(st.secrets["mysql"]["host"], st.secrets["mysql"]["port"]),
            )
        
        server.start()
        conn = mysql.connector.connect(
            host=st.secrets["mysql"]["host"],
            port=server.local_bind_port,
            database=st.secrets["mysql"]["database"],
            user=st.secrets["mysql"]["user"],
            password=mysql_password_local
        )
        return conn, server
    except Error as e:
        st.error(f"Error connecting to MySQL: {e}")
        return None
    
@st.cache_data
def load_reviews():
    conn, tunnel = get_connection()
    df = pd.read_sql("SELECT r.id AS `Id`, g.game_name AS `Game`, u.user_name AS `User`, r.review AS `Review`, CASE WHEN r.recommended = 1 THEN 'Yes' WHEN r.recommended = 0 THEN 'No' END AS `Recommended` FROM reviews AS r JOIN games AS g ON r.game_id = g.id JOIN users AS u ON u.id = r.user_id", conn)
    conn.close()
    tunnel.stop()
    return df

# Refreshes table.

def refresh_reviews():
    st.session_state.original_df = load_reviews()
    st.session_state.original_hash = hash_df(st.session_state.original_df)


# Converting ID's to Names and vice versa.

def get_game_id(game_name):
    conn, tunnel = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id FROM games WHERE game_name = %s",
        (game_name,)
    )
    row = cursor.fetchone()
    conn.close()
    tunnel.stop()
    return row[0] if row else None

def get_user_id(user_name):
    conn, tunnel = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id FROM users WHERE user_name = %s",
        (user_name,)
    )
    row = cursor.fetchone()
    conn.close()
    tunnel.stop()
    return row[0] if row else None

def get_game_name(game_id):
    conn, tunnel = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT game_name FROM games WHERE id = %s",
        (game_id,)
    )
    row = cursor.fetchone()
    conn.close()
    tunnel.stop()
    return row[0] if row else None

def get_user_name(user_id):
    conn, tunnel = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT user_name FROM users WHERE id = %s",
        (user_id,)
    )
    row = cursor.fetchone()
    conn.close()
    tunnel.stop()
    return row[0] if row else None

# Helper function to deduplicate columns
def dedupe_columns(df):
    seen = {}
    new_columns = []

    for col in df.columns:
        if col not in seen:
            seen[col] = 0
            new_columns.append(col)
        else:
            seen[col] += 1
            new_columns.append(f"{col}_{seen[col]}")

    df.columns = new_columns
    return df


# =============================
# Run Query Function
# =============================
@st.cache_data(show_spinner=False)
def run_query(sql: str, limit: int):
    """
    Executes a SQL query with a LIMIT clause
    and returns a pandas DataFrame.
    """
    if limit:
        sql = f"{sql} LIMIT {limit}"

    conn, tunnel = get_connection()
    df = pd.read_sql(sql, conn)
    conn.close()
    tunnel.stop()
    # Auto-fix duplicate columns (student-friendly)
    df = dedupe_columns(df)

    return df

#"SELECT * FROM airportdb.flight_view WHERE `date` BETWEEN '2015-08-01' and '2015-09-01'" OLD CHART

@st.cache_data
def load_chart_data():
    conn, tunnel = get_connection()
    df = pd.read_sql("SELECT g.game_name, COALESCE(SUM(r.recommended), 0) AS total_recommended FROM games AS g LEFT JOIN reviews AS r ON r.game_id = g.id GROUP BY g.id, g.game_name;", conn)
    conn.close()
    tunnel.stop()
    return df

def update_rows(updated_df, original_df):
    conn = get_connection()
    cursor = conn.cursor()
    
    for i, row in updated_df.iterrows():
        original_row = original_df.loc[i]
        if not row.equals(original_row):
            cursor.execute(
                "UPDATE passenger SET passportno=%s, firstname=%s, lastname=%s WHERE passenger_id=%s",
                (row['passportno'], row['firstname'], row['lastname'], row['passenger_id'])
            )
    conn.commit()
    conn.close()

def delete_rows(ids_to_delete):
    format_strings = ",".join( map(str, ids_to_delete))
    print(f"format_strings: {format_strings}")
    print(f"Deleting rows with IDs: {ids_to_delete}")
    conn, tunnel = get_connection()
    cursor = conn.cursor()

    cursor.execute(f"DELETE FROM reviews WHERE reviews.id IN ({format_strings})")
    conn.commit()
    conn.close()


def insert_row(review, recommended, game_name, user_name):
    conn, tunnel = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO reviews (review, recommended, game_id, user_id) VALUES (%s, %s, %s, %s)", (review, recommended, get_game_id(game_name), get_user_id(user_name)))
    conn.commit()
    conn.close()

# ---------- HELPER FUNCTIONS ----------

def hash_df(df):
    return hashlib.md5(pd.util.hash_pandas_object(df, index=True).values).hexdigest()

# ---------- STREAMLIT APP ----------

st.title("Game Data Dashboard")

# Line chart from DB view
st.subheader("📊 Game Recommendations")
df = load_chart_data()
st.bar_chart(
    df,
    x="game_name",
    y="total_recommended"
)

# Editable table
st.subheader(" Manage Reviews (Add, Edit, Delete)")

# Load reviews table
if "original_df" not in st.session_state:
    st.session_state.original_df = load_reviews()
    st.session_state.original_hash = hash_df(st.session_state.original_df)

df = st.session_state.original_df.copy()
df["delete"] = False

# edited_df = st.data_editor(df, num_rows="fixed", hide_index=True, column_order=("Id", "Game", "User", "Review", "Recommended"), use_container_width=True)
edited_df = st.data_editor(
    df,
    num_rows="fixed",
    hide_index=True,
    column_order=("Id", "Game", "User", "Review", "Recommended", "delete"),
    column_config={
        "delete": st.column_config.CheckboxColumn("Delete")
    },
    use_container_width=True
)


# Load initial data
if "original_df" not in st.session_state:
    st.session_state.original_df = load_reviews()
    st.session_state.original_hash = hash_df(st.session_state.original_df)

df = st.session_state.original_df.copy()

# Delete selected rows
if st.button("🗑️ Delete Selected Rows"):
    # selected_ids = edited_df[edited_df["delete"] == True]["Id"].tolist()
    selected_ids = st.session_state.editable_df[st.session_state.editable_df["delete"] == True]["Id"].tolist()
    if selected_ids:
        print(f"Deleting rows with IDs: {selected_ids}")
        delete_rows(selected_ids)
        st.session_state.original_df = load_reviews()
        st.session_state.original_hash = hash_df(st.session_state.original_df)
        st.success(f"Deleted {len(selected_ids)} row(s).")
        st.rerun()
    else:
        st.info("No rows selected for deletion.")

# # Save edits
# if st.button("💾 Save Edits"):
#     edited_df = edited_df.drop(columns=["delete"])
#     new_hash = hash_df(edited_df)
#     if new_hash != st.session_state.original_hash:
#         update_rows(edited_df, st.session_state.original_df)
#         st.session_state.original_df = edited_df
#         st.session_state.original_hash = new_hash
#         st.success("Changes saved.")
#     else:
#         st.info("No changes detected.")

# Insert new row
st.subheader("➕ Add New Review")
with st.form("insert_form"):
    user_name = st.text_input("Username")
    game = st.text_input("Game Name")
    review = st.text_input("Review")

    recommended = st.radio(
    "Recommended",
    options=[1, 0],                     # The actual values to use in your DB
    format_func=lambda x: "Yes" if x == 1 else "No"  # How it appears in the UI
)

    submitted = st.form_submit_button("Add Review")

    if submitted:
        if user_name.strip() == "" or game.strip() == "" or review.strip() == "" or recommended == "":
            st.warning("User ID, Game Name, Review, And Recommended are required.")
        elif get_user_id(user_name.strip()) is None:
            st.error("User does not exist.")
        elif get_game_id(game.strip()) is None:
            st.error("Game does not exist.")
        elif recommended not in (1, 0):
            st.error("Recommended must be 'Yes' or 'No'")
        else:
            insert_row(
                review.strip(),
                recommended,
                game.strip(),
                user_name.strip()
            )
            st.session_state.original_df = load_reviews()
            st.session_state.original_hash = hash_df(st.session_state.original_df)
            st.success(f"Review for '{game}' by '{user_name}' added.")
            st.rerun()

# Selectbox for requirement queries with limit slider

# =============================
# UI for queries
# =============================

st.title("SQL Query Explorer")
selected_option = st.selectbox(
    "Choose a SQL concept:",
    options=list(queries.keys())
)

# Row limit control
row_limit = st.slider(
    "Row limit",
    min_value=10,
    max_value=1000,
    value=100,
    step=10
)
# Show SQL toggle
with st.expander("Preview SQL"):
    st.code(queries[selected_option], language="sql")

# Run Query Button
if st.button("Run Query", type="primary"):
    with st.spinner("Running query..."):
        try:
            df = run_query(queries[selected_option], row_limit)

            st.success(f"Results for **{selected_option}**")
            st.dataframe(df, use_container_width=True)

            st.caption(f"Rows returned: {len(df)}")

        except Exception as e:
            st.error("Query execution failed")
            st.exception(e)



# End of Streamlit app
