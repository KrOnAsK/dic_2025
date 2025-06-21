(function ($) {
    let functionUrlPresign = localStorage.getItem("functionUrlPresign");
    if (functionUrlPresign) {
        console.log("function url presign is", functionUrlPresign);
        $("#functionUrlPresign").val(functionUrlPresign);
    }

    let functionUrlList = localStorage.getItem("functionUrlList");
    if (functionUrlList) {
        console.log("function url list is", functionUrlList);
        $("#functionUrlList").val(functionUrlList);
    }
    let functionUrlDBList = localStorage.getItem("functionUrlDBList");
    if (functionUrlDBList) {
        console.log("function url db_list is", functionUrlDBList);
        $("#functionUrlDBList").val(functionUrlDBList);
    }

    let imageItemTemplate = Handlebars.compile($("#image-item-template").html());

    $("#configForm").submit(async function (event) {
        if (event.preventDefault)
            event.preventDefault();
        else
            event.returnValue = false;

        event.preventDefault();
        let action = $(this).find("button[type=submit]:focus").attr('name');
        if (action === undefined) {
            // the jquery find with the focus does not work on Safari, maybe because the focus is not instantly given
            // fallback to manually retrieving the submitter from the original event
            action = event.originalEvent.submitter.getAttribute('name')
        }

        if (action == "load") {
            let baseUrl = `${document.location.protocol}//${document.location.host}`;
            if (baseUrl.indexOf("file://") >= 0) {
                baseUrl = `http://localhost:4566`;
            }
            baseUrl = baseUrl.replace("://review-webapp.s3.", "://").replace("://review-webapp.s3-website.", "://");
            const headers = {authorization: "AWS4-HMAC-SHA256 Credential=test/20231004/us-east-1/lambda/aws4_request, ..."};
            const loadUrl = async (funcName, resultElement) => {
                const url = `${baseUrl}/2021-10-31/functions/${funcName}/urls`;
                console.log("loading function URL for", funcName, "from", url);
                const result = await $.ajax({url, headers}).promise();
                console.log("result for", funcName, result);
                const funcUrl = JSON.parse(result).FunctionUrlConfigs[0].FunctionUrl;
                console.log("funcUrl", funcUrl);
                $(`#${resultElement}`).val(funcUrl);
                localStorage.setItem(resultElement, funcUrl);
            }
            await loadUrl("presign", "functionUrlPresign");
            await loadUrl("list", "functionUrlList");
            await loadUrl("db_list", "functionUrlDBList")
            alert("Function URL configurations loaded");
        } else if (action == "save") {
            localStorage.setItem("functionUrlPresign", $("#functionUrlPresign").val());
            localStorage.setItem("functionUrlList", $("#functionUrlList").val());
            localStorage.setItem("functionUrlDBList", $("#functionUrlDBList").val());
            alert("Configuration saved");
        } else if (action == "clear") {
            localStorage.removeItem("functionUrlPresign");
            localStorage.removeItem("functionUrlList");
            localStorage.removeItem("functionUrlDBList");
            $("#functionUrlPresign").val("")
            $("#functionUrlList").val("")
            $("#functionUrlDBList").val("")
            alert("Configuration cleared");
        } else {
            alert("Unknown action");
        }

    });

    $("#uploadForm").submit(function (event) {
        $("#uploadForm button").addClass('disabled');

        console.log("upload form submitted", event);
        if (event.preventDefault)
            event.preventDefault();
        else
            event.returnValue = false;

        event.preventDefault();

        let fileName = $("#customFile").val().replace(/C:\\fakepath\\/i, '');
        console.log("fileName", fileName);
        let functionUrlPresign = $("#functionUrlPresign").val();
        console.log("functionUrlPresign", functionUrlPresign);
        // modify the original form
        console.log(fileName, functionUrlPresign);

        let urlToCall = functionUrlPresign
        
        console.log(urlToCall);
        console.log("$AJAX command starting")
        $.ajax({
            url: urlToCall,
            success: function (data) {
                console.log("got pre-signed POST URL");
                // rest of your code
            },
            error: function(xhr, status, error) {
                console.error("Error getting pre-signed POST URL:", status, error);
                alert("Failed to get pre-signed URL. See console for details.");
                $("#uploadForm button").removeClass('disabled');
            }
        });
        $.ajax({
            url: urlToCall,
            success: function (data) {
                console.log("got pre-signed POST URL");
                console.log("got pre-signed POST URL", data);

                let fields = data['fields'];

                let formData = new FormData()
                
                Object.entries(fields).forEach(([field, value]) => {
                    formData.append(field, value);
                });

                // the file <input> element, "file" needs to be the last element of the form
                const fileElement = document.querySelector("#customFile");
                // Add this before appending the file
                formData.append("Content-Type", "application/json");

                formData.append("file", fileElement.files[0]);

                console.log("sending form data", formData);
                alert("Uploading file: " + fileName);
                $.ajax({
                    type: "POST",
                    url: data['url'],
                    data: formData,
                    processData: false,
                    contentType: false,
                    success: function () {
                        alert("success!");
                        updateReviewList();
                    },
                    error: function () {
                        alert("error! check the logs");
                    },
                    complete: function (event) {
                        console.log("done", event);
                        $("#uploadForm button").removeClass('disabled');
                    }
                });
            },
            error: function (e) {
                console.log("error", e);
                alert("error getting pre-signed URL. check the logs!");
                $("#uploadForm button").removeClass('disabled');
            }
        });
    });

    function updateReviewList() {
        let listUrl = $("#functionUrlList").val();
        if (!listUrl) {
            alert("Please set the function URL of the list Lambda");
            return
        }

        $.ajax({
            url: listUrl,
            success: function (response) {
                $('#imagesContainer').empty(); // Empty imagesContainer
                response.forEach(function (item) {
                    console.log(item);
                    let cardHtml = imageItemTemplate(item);
                    $("#imagesContainer").append(cardHtml);
                });
            },
            error: function (jqXHR, textStatus, errorThrown) {
                console.log("Error:", textStatus, errorThrown);
                alert("error! check the logs");
            }
        });
    }

    $("#updateReviewListButton").click(function (event) {
        updateReviewList();
    });
    $("#updateDBButton").click(function() {
        updateDBList();
    });

    if (functionUrlList) {
        updateReviewList();
    }
    if (functionUrlDBList) {
        updateReviewList();
    }

})(jQuery);
