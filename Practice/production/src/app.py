import streamlit as st
import pandas as pd
from model import infer
from main import mainloop
from preprocessing import dataload


def set_cfg():
    st.set_page_config(
        page_title="Predict Welding joint size",
        page_icon="🌟",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.title("Welding joint")
    st.sidebar.header("Parameters")
    st.sidebar.subheader("Enter Parameters Values:")


def get_params(df):
    iw_input = st.sidebar.number_input("Enter (IW):", min_value=df['IW'].min(), max_value=df['IW'].max())
    vw_input = st.sidebar.number_input("Enter (VW):", min_value=df['VW'].min(), max_value=df['VW'].max())
    fp_input = st.sidebar.number_input("Enter (FP):", min_value=df['FP'].min(), max_value=df['FP'].max())
    if_input = st.sidebar.number_input("Enter (IF):", min_value=df['IF'].min(), max_value=df['IF'].max())
    return iw_input, vw_input, fp_input


def st_predict(df, input_data):
    model1, model2 = mainloop(df)
    prediction1 = round(infer(model1, input_data)[0], 3)
    prediction2 = round(infer(model2, input_data)[0], 3)
    return prediction1, prediction2


def get_img(prediction, pic, name='Depth'):
    st.subheader('Welding...')
    local_image_path = f"data/{pic}.gif"
    st.image(local_image_path, caption="welding", use_column_width=True)
    st.markdown(f"## Predicted{name}: <span style='font-size:36px'>{str(prediction)}</span>", unsafe_allow_html=True)


def get_result(df, input_data):
    prediction1, prediction2 = st_predict(df, input_data)
    get_img(prediction1, 'welding_1')
    st.markdown("<hr style='border:2px solid black'>", unsafe_allow_html=True)
    get_img(prediction2, 'welding_3', 'Width')


def main():
    set_cfg()
    print('We are importing data from data/ebw_data.csv/n/n')
    df = dataload('data/ebw_data.csv')

    iw_input, vw_input, fp_input = get_params(df)

    if st.sidebar.button("Predict"):

        st.success("Prediction Successful!")

        input_data = pd.DataFrame({'IW': [iw_input], 'VW': [vw_input], 'FP': [fp_input]})
        get_result(df, input_data)

if __name__ == '__main__':
    main()