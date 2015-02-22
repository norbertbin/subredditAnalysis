library(shiny)
library(ggvis)

### two row layout with plots on top and input on bottom
shinyUI(fluidPage(
    br(),
    
    wellPanel(titlePanel("Exploring the IAmA Subreddit!", "ShIAmA"),

    tags$div(class="header",
             tags$p("The purpose of this app is to explore some submission and comment data from the IAmA subreddit. The data was pulled from the top 995 hot submissions to the IAmA subreddit as ranked by reddit on 02/20/2015. A word, user, or submission can be explored via the app by selecting one of the three radio buttons. The desired word, user id, or submission id should then be entered in the text box. After the update button is pressed, a cloud representing word frequency or user usage frequency will be drawn. The tabs above the graph can be used to switch between a word cloud representation and a bar graph.")
             )),
    
    fluidRow(
        column(3, wellPanel(
               radioButtons("type", "Select:", c("Word" = "word", "User" = "user",
                                               "Submission" = "submission")),
               textInput("label", "Word or Id:", "read"),
               submitButton("Update Graphs"),
            br(),
            tags$div(class="header",
               tags$p("User ids: 0 - 39129")),
            tags$div(class="header",
               tags$p("Submission ids: 0 - 994"))
        )),
        column(9, wellPanel(
            tabsetPanel(
                tabPanel("Cloud", plotOutput('plot1')),
                tabPanel("Bar Graph", ggvisOutput('plot2'))
            )
        ))
    )

))

