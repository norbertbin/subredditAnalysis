library(shiny)
library(ggvis)
library(dplyr)
library(R.matlab)
library(RSQLite)
library(wordcloud)

### constants
PROC_DB_NAME = "./data/proc_subreddit.db"
DTM_FILE = "./data/dtm_subreddit.mat"
MAX_ROWS = 15
###


### helper functions
# gets the counts for a word in comments/submissions
get_word_freq <- function(dtm, vocab, word) {
    word_id = which(vocab == word)
    dtm = summary(dtm)
    dtm = dtm[dtm$j == word_id,]
    df = data.frame(item = dtm$i - 1, freq = dtm$x) %>% arrange(desc(freq))
    return( df[1:min(MAX_ROWS, nrow(df)),] )
}

# gets the counts for the top words in entity (comments/submissions)
get_word_for_entity <- function(dtm, vocab, entity, entity_array) {
    entity_id = which(entity_array == entity)
    dtm = summary(dtm)
    dtm = dtm[dtm$i == entity_id,]
    df = data.frame(item = vocab[dtm$j], freq = dtm$x) %>% arrange(desc(freq))
    return( df[1:min(MAX_ROWS, nrow(df)),] )
}

get_sub_for_user <- function(comments, user) {
    sub_df = comments %>% filter(author == user) %>% group_by(submission_id) %>% summarise(count = n()) %>% arrange(desc(count)) %>% collect()
    sub_df = sub_df[1:min(MAX_ROWS, nrow(sub_df)),]
    return( data.frame(item = sub_df$submission_id, freq = sub_df$count) )
}

get_user_for_sub <- function(comments, sub) {
    sub_df = comments %>% filter(submission_id == sub) %>% group_by(author) %>% summarise(count = n()) %>% arrange(desc(count)) %>% collect()
    sub_df = sub_df[1:min(MAX_ROWS, nrow(sub_df)),]
    return( data.frame(item = sub_df$author, freq = sub_df$count) )
}

# checks if user input is in the allowed set of inputs
valid_input <- function(input, allowed_input) {
    return( input %in% allowed_input )
}
###


### load data
dtm_data = readMat(DTM_FILE)
proc_db = src_sqlite(PROC_DB_NAME)
vocab = tbl(proc_db, "Vocab")
submissions = tbl(proc_db, "Submissions")
comments = tbl(proc_db, "Comments")
users = tbl(proc_db, "User")
vocab = (vocab %>% select(vocab) %>% collect())$vocab
users = (users %>% select(user) %>% collect())$user
submissions = (submissions %>% select(sub_id) %>% collect())$sub_id
###


### create interactive plots with ggvis
shinyServer(function(input, output, session) {
    
    # get the right data based on radio button input
    data <- reactive({
        if(input$type == "word") {
            validate(
                need(valid_input(input$label, vocab), "Word Not Found")
            )
            list(d1 = get_word_freq(dtm_data$user.dtm, vocab, input$label),
                 d2 = get_word_freq(dtm_data$sub.dtm, vocab, input$label),
                 xlab1 = "User", xlab2 = "Submission")
        } else if (input$type == "user") {
            validate(
                need(valid_input(input$label, users), "User Not Found")
            )
            list(d1 = get_word_for_entity(dtm_data$user.dtm, vocab, input$label, users),
                 d2 = get_sub_for_user(comments, input$label),
                 xlab1 = "Word", xlab2 = "Submission")
        } else {
            validate(
                need(valid_input(input$label, submissions), "Submission Not Found")
            )
            list(d1 = get_word_for_entity(dtm_data$sub.dtm, vocab, input$label, submissions),
                 d2 = get_user_for_sub(comments, input$label),
                 xlab1 = "Word", xlab2 = "User")
        }
    })
    
   reactive({ data()$d1 %>%
        ggvis(~factor(item, levels = item), ~freq) %>%
            layer_bars(fill := "darkblue") %>%
                add_axis("x", title = data()$xlab1,
                         properties = axis_props(labels = list(angle = 45))) %>%
                             add_axis("y", title = "Count") %>%
                             set_options(width = 600, height = 300) }) %>%
                    bind_shiny("plot2", "plot_ui")

    output$plot1 <- renderPlot({
        dd <- data()$d1
        wordcloud(dd$item, dd$freq, min.freq=1, scale=c(5,.5), rot.per=.3, colors=brewer.pal(8,"Dark2"))
    })
    
   ## reactive({data()$d2 %>%
   ##      ggvis(~factor(item, levels = item), ~freq) %>%
   ##          layer_bars(fill := "darkblue") %>%
   ##              add_axis("x", title = data()$xlab2,
   ##                       properties = axis_props(labels = list(angle = 45))) %>%
   ##                           add_axis("y", title = "Count") %>%
   ##                           set_options(width = 600, height = 300)}) %>%
   ##                  bind_shiny("plot3", "plot_ui")
})    
###
